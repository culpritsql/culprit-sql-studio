//! Borrow one held transport generation without taking login, cancellation or close ownership.
use super::*;

pub(crate) struct SameConnection<'a, R> {
    reader: &'a mut R,
    generation: u64,
}
impl<'a, R: QueryRead> SameConnection<'a, R> {
    pub(crate) fn new(reader: &'a mut R, generation: u64) -> Result<Self> {
        if generation == 0 || reader.connection_generation() != Some(generation) {
            return Err(CollectionStop::SourceChanged.error());
        }
        Ok(Self { reader, generation })
    }
    fn check(&self) -> Result<()> {
        (self.reader.connection_generation() == Some(self.generation))
            .then_some(())
            .ok_or_else(|| CollectionStop::SourceChanged.error())
    }
}
#[async_trait::async_trait]
impl<R: QueryRead> QueryRead for SameConnection<'_, R> {
    fn connection_generation(&self) -> Option<u64> {
        self.reader.connection_generation()
    }
    async fn read(&mut self, sql: &str, parameters: Vec<Value>, row_limit: usize) -> Result<Vec<Rec>> {
        self.check()?;
        let rows = self.reader.read(sql, parameters, row_limit).await?;
        self.check()?;
        Ok(rows)
    }
    async fn explain(&mut self, sql: &str) -> Result<Option<Rec>> {
        self.check()?;
        let row = self.reader.explain(sql).await?;
        self.check()?;
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::test_support::Read;
    #[tokio::test]
    async fn late_generation_change_withholds_rows_consumes_work_and_never_opens_a_replacement() {
        let mut reader = Read::with(vec![Ok(vec![])]).with_native_phases().with_query_budget(1, 0);
        reader.change_generation_after = Some(1);
        let mut held = SameConnection::new(&mut reader, 1).unwrap();
        assert_eq!(
            CollectionStop::from_error(&held.read("SELECT exact_native_point", vec![], 1).await.err().unwrap()),
            CollectionStop::SourceChanged
        );
        assert_eq!(
            CollectionStop::from_error(&held.read("SELECT forbidden_replacement", vec![], 1).await.err().unwrap()),
            CollectionStop::SourceChanged
        );
        drop(held);
        assert_eq!(reader.queries.len(), 1);
        assert_eq!(reader.query_count(), 1);
    }
    #[tokio::test]
    async fn first_native_error_survives_transport_loss_and_unknown_transport_admits_no_work() {
        let mut reader = Read::with(vec![Err(CollectionStop::Cancelled.error())]);
        reader.change_generation_after = Some(1);
        let mut held = SameConnection::new(&mut reader, 1).unwrap();
        assert_eq!(
            CollectionStop::from_error(&held.read("SELECT original_error", vec![], 1).await.err().unwrap()),
            CollectionStop::Cancelled
        );
        drop(held);
        assert_eq!(reader.queries.len(), 1);
        reader.generation = None;
        assert!(SameConnection::new(&mut reader, 1).is_err());
        assert_eq!(reader.queries.len(), 1);
    }
}
