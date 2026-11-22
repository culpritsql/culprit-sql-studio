use super::evidence::RowReceipt;
#[derive(Clone, Debug)]
pub struct ReceiptPage {
    pub rows: Vec<RowReceipt>,
    pub next_cursor: Option<String>,
    pub complete: bool,
}
