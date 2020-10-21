use dgraph_tonic::{TxnContext, ILazyClient, TxnMutatedType};

pub trait TxnContextExport {
    fn txn_context(&self) -> TxnContext;
}

impl<C: ILazyClient> TxnContextExport for TxnMutatedType<C> {
    fn txn_context(&self) -> TxnContext {
        self.context.clone()
    }
}
