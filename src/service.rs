use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub trait Service<Command, Event> {
    type Params;

    fn start(
        token: CancellationToken,
        command_rx: Option<mpsc::Receiver<Command>>,
        event_tx: Option<mpsc::Sender<Event>>,
        params: Self::Params,
    ) -> anyhow::Result<()>;

    async fn run(self);
}
