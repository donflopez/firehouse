#[macro_export]
macro_rules! s {
    ($e: tt) => {
        String::from($e);
    };
}
