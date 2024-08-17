pub fn assert_send<T: Send>(x: T) -> T {
    x
}
