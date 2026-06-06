use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type AsyncBoxFuture<'a, O> = Pin<Box<dyn Future<Output = O> + Send + 'a>>;

pub enum ClientResolverInner<T: Clone> {
    Defered(Option<Box<dyn Fn() -> AsyncBoxFuture<'static, Result<T>> + Send + Sync>>),
    Resolved(T),
}

#[derive(Clone)]
pub struct ClientResolver<T: Clone> {
    state: Arc<Mutex<ClientResolverInner<T>>>,
}

impl<T: Clone + Send + Sync + 'static> ClientResolver<T> {
    pub fn new_lazy<F>(closure: F) -> Self
    where
        F: Fn() -> AsyncBoxFuture<'static, Result<T>> + Send + Sync + 'static,
    {
        Self {
            state: Arc::new(Mutex::new(ClientResolverInner::Defered(Some(Box::new(
                closure,
            ))))),
        }
    }

    pub fn new_resolved(instance: T) -> Self {
        Self {
            state: Arc::new(Mutex::new(ClientResolverInner::Resolved(instance))),
        }
    }

    pub async fn resolve(&self) -> Result<T> {
        let mut lock = self.state.lock().await;
        match &mut *lock {
            ClientResolverInner::Resolved(instance) => Ok(instance.clone()),
            ClientResolverInner::Defered(resolve_fn_opt) => {
                let resolve_fn = resolve_fn_opt.take().expect("expeted resolver funtion");
                match resolve_fn().await {
                    Ok(instance) => {
                        *lock = ClientResolverInner::Resolved(instance.clone());
                        Ok(instance)
                    }
                    Err(err) => {
                        *resolve_fn_opt = Some(resolve_fn);
                        Err(err)
                    }
                }
            }
        }
    }
}
