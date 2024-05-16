use derive_new::new;
use std::io::Error;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};

use pin_project::pin_project;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite, BufWriter, ReadBuf};

use crate::persistent::interface::WalHandle;

#[derive(new)]
#[pin_project]
pub struct WalFile(#[pin] BufWriter<File>);

impl AsyncWrite for WalFile {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.project();
        this.0.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        this.0.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let this = self.project();
        this.0.poll_shutdown(cx)
    }
}

impl AsyncRead for WalFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.project();
        this.0.poll_read(cx, buf)
    }
}

impl WalHandle for WalFile {}
