//! app-server stdin/stdout/stderr pipe 上の transport task を定義する。
//!
//! stdin と stdout は一行一 JSON value の JSONL protocol を使用する。stdout reader は newline が届かない
//!場合でも buffer growth を `max_message_bytes` で制限し、child process が memory exhaustion を起こせないようにする。
//! stderr は protocol と解釈せず raw byte stream として drain する。

use std::{fmt, future::Future};

use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{mpsc, oneshot},
};

use crate::ClientError;

/// single stdin writer task へ送る command。
pub(crate) enum Outgoing {
    /// JSON-RPC message を一行として書き込む。
    Message {
        value: Value,
        /// `Some` の場合は flush 完了または write failure を caller へ返す。
        ack: Option<oneshot::Sender<Result<(), ClientError>>>,
    },
    /// queue 内の先行 message を処理した後に stdin pipe を close する。
    Close {
        ack: oneshot::Sender<Result<(), ClientError>>,
    },
}

impl fmt::Debug for Outgoing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message { ack, .. } => formatter
                .debug_struct("Message")
                // request parameter、prompt、approval response を含み得るため payload は公開しない。
                .field("value", &"<redacted>")
                .field("ack", &ack.is_some())
                .finish(),
            Self::Close { .. } => formatter.write_str("Close"),
        }
    }
}

/// outgoing queue を順序どおり stdin へ書き込む唯一の writer task。
///
///複数 task が pipe へ直接 write すると JSONL frame が interleave するため、serialization、newline 付与、flush、
///close をこの task に集約する。queue sender がすべて drop された場合も stdin を shutdown する。
pub(crate) async fn stdin_writer<W>(
    mut writer: W,
    mut receiver: mpsc::Receiver<Outgoing>,
) -> Result<(), ClientError>
where
    W: AsyncWrite + Unpin,
{
    while let Some(outgoing) = receiver.recv().await {
        match outgoing {
            Outgoing::Message { value, ack } => {
                let result = write_message(&mut writer, &value).await;
                if let Some(ack) = ack {
                    let _ = ack.send(result.clone());
                }
                result?;
            }
            Outgoing::Close { ack } => {
                let result =
                    writer
                        .shutdown()
                        .await
                        .map_err(|error| ClientError::StdinWriteFailed {
                            detail: error.to_string(),
                        });
                let _ = ack.send(result.clone());
                return result;
            }
        }
    }
    writer
        .shutdown()
        .await
        .map_err(|error| ClientError::StdinWriteFailed {
            detail: error.to_string(),
        })
}

///一つの JSON value を compact JSON、LF の順で書き込み、pipe を flush する。
///
/// acknowledgement が必要な initialize や ServerRequest response は、この flush 完了を protocol 上の送信完了とする。
async fn write_message<W>(writer: &mut W, value: &Value) -> Result<(), ClientError>
where
    W: AsyncWrite + Unpin,
{
    let mut bytes =
        serde_json::to_vec(value).map_err(|error| ClientError::SerializationFailed {
            operation: "JSON-RPC message",
            detail: error.to_string(),
        })?;
    bytes.push(b'\n');
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| ClientError::StdinWriteFailed {
            detail: error.to_string(),
        })?;
    writer
        .flush()
        .await
        .map_err(|error| ClientError::StdinWriteFailed {
            detail: error.to_string(),
        })
}

/// stdout byte stream を bounded JSONL frame に分割し、deserialize 済み value を dispatcher へ渡す。
///
/// `fill_buf` 単位で newline を探索し、frame が完結する前でも `max_message_bytes + CRLF` を超えた時点で拒否する。
/// EOF 時に data が残っていれば、最後の unterminated frame として一度だけ処理する。
pub(crate) async fn stdout_reader<R, F, Fut>(
    reader: R,
    max_message_bytes: usize,
    mut dispatch: F,
) -> Result<(), ClientError>
where
    R: AsyncRead + Unpin,
    F: FnMut(Value) -> Fut,
    Fut: Future<Output = Result<(), ClientError>>,
{
    let mut reader = BufReader::new(reader);
    //通常 frame で過剰予約しない一方、設定値までの growth は明示的な上限判定で管理する。
    let mut frame = Vec::with_capacity(max_message_bytes.min(8192));
    // payload limit に LF または CRLF の最大 2 byte だけを加えた transport 上限。
    let frame_limit = max_message_bytes.saturating_add(2);

    loop {
        frame.clear();
        loop {
            let buffer =
                reader
                    .fill_buf()
                    .await
                    .map_err(|error| ClientError::StdoutReadFailed {
                        detail: error.to_string(),
                    })?;
            if buffer.is_empty() {
                if frame.is_empty() {
                    return Ok(());
                }
                let value = parse_frame(&frame, max_message_bytes)?;
                dispatch(value).await?;
                return Ok(());
            }

            if let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
                let consumed = newline + 1;
                if frame.len().saturating_add(consumed) > frame_limit {
                    return Err(ClientError::MessageTooLarge {
                        limit: max_message_bytes,
                    });
                }
                frame.extend_from_slice(&buffer[..consumed]);
                reader.consume(consumed);
                break;
            }

            if frame.len().saturating_add(buffer.len()) > frame_limit {
                return Err(ClientError::MessageTooLarge {
                    limit: max_message_bytes,
                });
            }
            let consumed = buffer.len();
            frame.extend_from_slice(buffer);
            reader.consume(consumed);
        }

        let value = parse_frame(&frame, max_message_bytes)?;
        dispatch(value).await?;
    }
}

/// LF または CRLF を除去し、size、UTF-8、non-empty JSON の順で一つの frame を検証する。
///
/// whitespace-only frame は JSON parser の error に委ねず、明示的な protocol violation として扱う。
pub(crate) fn parse_frame(frame: &[u8], max_message_bytes: usize) -> Result<Value, ClientError> {
    let mut payload = frame;
    if payload.last() == Some(&b'\n') {
        payload = &payload[..payload.len() - 1];
        if payload.last() == Some(&b'\r') {
            payload = &payload[..payload.len() - 1];
        }
    }
    if payload.len() > max_message_bytes {
        return Err(ClientError::MessageTooLarge {
            limit: max_message_bytes,
        });
    }
    let text =
        std::str::from_utf8(payload).map_err(|error| ClientError::DeserializationFailed {
            operation: "stdout UTF-8 frame",
            detail: error.to_string(),
        })?;
    if text.trim().is_empty() {
        return Err(ClientError::ProtocolViolation {
            reason: "stdout contained an empty JSONL frame".to_owned(),
        });
    }
    serde_json::from_str(text).map_err(|error| ClientError::DeserializationFailed {
        operation: "stdout JSON frame",
        detail: error.to_string(),
    })
}

/// stderr を protocol parsing せず固定長 chunk で drain する。
///
/// stderr を読まないと child process が pipe backpressure で停止し得る。byte chunk は UTF-8 や line boundary を
///保証せず、そのまま Event::Stderr として caller へ渡す。
pub(crate) async fn stderr_reader<R, F>(mut reader: R, mut publish: F) -> Result<(), ClientError>
where
    R: AsyncRead + Unpin,
    F: FnMut(Vec<u8>),
{
    let mut buffer = vec![0; 8192];
    loop {
        let read =
            reader
                .read(&mut buffer)
                .await
                .map_err(|error| ClientError::StdoutReadFailed {
                    detail: format!("stderr drain failed: {error}"),
                })?;
        if read == 0 {
            return Ok(());
        }
        publish(buffer[..read].to_vec());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[test]
    fn parses_lf_crlf_and_final_unterminated_frames() {
        assert_eq!(parse_frame(b"{\"a\":1}\n", 64).unwrap(), json!({"a": 1}));
        assert_eq!(parse_frame(b"{\"a\":1}\r\n", 64).unwrap(), json!({"a": 1}));
        assert_eq!(parse_frame(b"{\"a\":1}", 64).unwrap(), json!({"a": 1}));
    }

    #[test]
    fn rejects_empty_invalid_utf8_invalid_json_and_oversize() {
        assert!(matches!(
            parse_frame(b" \n", 64),
            Err(ClientError::ProtocolViolation { .. })
        ));
        assert!(matches!(
            parse_frame(&[0xff, b'\n'], 64),
            Err(ClientError::DeserializationFailed { .. })
        ));
        assert!(matches!(
            parse_frame(b"{broken}\n", 64),
            Err(ClientError::DeserializationFailed { .. })
        ));
        assert!(matches!(
            parse_frame(b"{}\n", 1),
            Err(ClientError::MessageTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn handles_multiple_messages_split_across_read_boundaries() {
        let (mut writer, reader) = tokio::io::duplex(8);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let task_seen = seen.clone();
        let reader_task = tokio::spawn(async move {
            stdout_reader(reader, 128, |value| {
                task_seen.lock().unwrap().push(value);
                std::future::ready(Ok(()))
            })
            .await
        });
        writer.write_all(b"{\"a\":").await.unwrap();
        writer.write_all(b"1}\n{\"b\":2}\n").await.unwrap();
        writer.shutdown().await.unwrap();
        reader_task.await.unwrap().unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            vec![json!({"a": 1}), json!({"b": 2})]
        );
    }

    #[tokio::test]
    async fn rejects_oversized_unterminated_frame_without_unbounded_growth() {
        let (mut writer, reader) = tokio::io::duplex(16);
        let reader_task =
            tokio::spawn(
                async move { stdout_reader(reader, 8, |_| std::future::ready(Ok(()))).await },
            );
        writer.write_all(b"12345678901").await.unwrap();
        assert!(matches!(
            reader_task.await.unwrap(),
            Err(ClientError::MessageTooLarge { limit: 8 })
        ));
    }

    #[tokio::test]
    async fn writer_uses_compact_json_lf_and_flush() {
        let (writer, mut reader) = tokio::io::duplex(128);
        let (sender, receiver) = mpsc::channel(1);
        let task = tokio::spawn(stdin_writer(writer, receiver));
        let (ack_sender, ack_receiver) = oneshot::channel();
        sender
            .send(Outgoing::Message {
                value: json!({"a": [1, 2]}),
                ack: Some(ack_sender),
            })
            .await
            .unwrap();
        ack_receiver.await.unwrap().unwrap();
        let mut line = String::new();
        let mut buffered = BufReader::new(&mut reader);
        buffered.read_line(&mut line).await.unwrap();
        assert_eq!(line, "{\"a\":[1,2]}\n");
        drop(sender);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn stdin_write_failure_is_reported() {
        let (writer, reader) = tokio::io::duplex(8);
        drop(reader);
        let (sender, receiver) = mpsc::channel(1);
        let task = tokio::spawn(stdin_writer(writer, receiver));
        sender
            .send(Outgoing::Message {
                value: json!({"method": "test"}),
                ack: None,
            })
            .await
            .unwrap();
        drop(sender);
        assert!(matches!(
            task.await.unwrap(),
            Err(ClientError::StdinWriteFailed { .. })
        ));
    }
}
