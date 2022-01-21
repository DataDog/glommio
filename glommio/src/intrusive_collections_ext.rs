/// Macro to generate an implementation of `Adapter` from
/// `intrusive_collections` crate for a given set of types.
/// In particular this will automatically generate implementations of the
/// `get_value` and `get_link` methods for a given named field in a struct.
///
/// Unlike the macro provided by `intrusive_collections`
/// this macro does not implement [`Send`] and [`Sync`] and can be used with
/// containers that do not implement those traits without generation of unsound
/// code.
///
/// The basic syntax to create an adapter is:
///
/// ```rust,ignore
/// intrusive_adapter_no_send!(Adapter = Pointer: Value { link_field: LinkType });
/// ```
///
/// You can create a new instance of an adapter using the `new` method or the
/// `NEW` associated constant. The adapter also implements the `Default` trait.
///
/// # Generics
///
/// This macro supports generic arguments:
///
/// ```rust,ignore
/// intrusive_adapter_no_send!(
///     Adapter<'lifetime, Type, Type2> =
///         Pointer: Value {
///             link_field: LinkType
///         }
///         where
///             Type: Copy,
///             Type2: ?Sized + 'lifetime
///     );
/// ```
///
/// Note that due to macro parsing limitations, `T: Trait` bounds are not
/// supported in the generic argument list. You must list any trait bounds in
/// a separate `where` clause at the end of the macro.
///
/// # Examples
///
/// ```
/// use intrusive_collections::{LinkedListLink, RBTreeLink};
/// use glommio::intrusive_adapter_no_send;
///
/// pub struct Test {
///     link: LinkedListLink,
///     link2: RBTreeLink,
/// }
/// intrusive_adapter_no_send!(MyAdapter = Box<Test>: Test { link: LinkedListLink });
/// intrusive_adapter_no_send!(pub MyAdapter2 = Box<Test>: Test { link2: RBTreeLink });
/// intrusive_adapter_no_send!(pub(crate) MyAdapter3 = Box<Test>: Test { link2: RBTreeLink });
///
/// pub struct Test2<T>
///     where T: Clone + ?Sized
/// {
///     link: LinkedListLink,
///     val: T,
/// }
/// intrusive_adapter_no_send!(MyAdapter4<'a, T> = &'a Test2<T>: Test2<T> { link: LinkedListLink } where T: ?Sized + Clone + 'a);
/// ```
#[macro_export]
macro_rules! intrusive_adapter_no_send {
    (@impl
        $(#[$attr:meta])* $vis:vis $name:ident ($($args:tt),*)
        = $pointer:ty: $value:path { $field:ident: $link:ty } $($where_:tt)*
    ) => {
        #[allow(explicit_outlives_requirements)]
        $(#[$attr])*
        $vis struct $name<$($args),*> $($where_)* {
            link_ops: <$link as intrusive_collections::DefaultLinkOps>::Ops,
            pointer_ops: intrusive_collections::DefaultPointerOps<$pointer>,
        }
        impl<$($args),*> Copy for $name<$($args),*> $($where_)* {}
        impl<$($args),*> Clone for $name<$($args),*> $($where_)* {
            #[inline]
            fn clone(&self) -> Self {
                *self
            }
        }
        impl<$($args),*> Default for $name<$($args),*> $($where_)* {
            #[inline]
            fn default() -> Self {
                Self::NEW
            }
        }
        #[allow(dead_code)]
        impl<$($args),*> $name<$($args),*> $($where_)* {
            pub const NEW: Self = $name {
                link_ops: <$link as intrusive_collections::DefaultLinkOps>::NEW,
                pointer_ops: intrusive_collections::DefaultPointerOps::<$pointer>::new(),
            };
            #[inline]
            pub fn new() -> Self {
                Self::NEW
            }
        }
        #[allow(dead_code, unsafe_code)]
        unsafe impl<$($args),*> intrusive_collections::Adapter for $name<$($args),*> $($where_)* {
            type LinkOps = <$link as intrusive_collections::DefaultLinkOps>::Ops;
            type PointerOps = intrusive_collections::DefaultPointerOps<$pointer>;

            #[inline]
            unsafe fn get_value(&self, link: <Self::LinkOps as
            intrusive_collections::LinkOps>::LinkPtr) ->
            *const <Self::PointerOps as intrusive_collections::PointerOps>::Value {
                intrusive_collections::container_of!(link.as_ptr(), $value, $field)
            }
            #[inline]
            unsafe fn get_link(&self, value: *const <Self::PointerOps as
            intrusive_collections::PointerOps>::Value)
            -> <Self::LinkOps as intrusive_collections::LinkOps>::LinkPtr {
                // We need to do this instead of just accessing the field directly
                // to strictly follow the stack borrow rules.
                let ptr = (value as *const u8).add(intrusive_collections::offset_of!($value, $field));
                core::ptr::NonNull::new_unchecked(ptr as *mut _)
            }
            #[inline]
            fn link_ops(&self) -> &Self::LinkOps {
                &self.link_ops
            }
            #[inline]
            fn link_ops_mut(&mut self) -> &mut Self::LinkOps {
                &mut self.link_ops
            }
            #[inline]
            fn pointer_ops(&self) -> &Self::PointerOps {
                &self.pointer_ops
            }
        }
    };
    (@find_generic
        $(#[$attr:meta])* $vis:vis $name:ident ($($prev:tt)*) > $($rest:tt)*
    ) => {
        intrusive_adapter_no_send!(@impl
            $(#[$attr])* $vis $name ($($prev)*) $($rest)*
        );
    };
    (@find_generic
        $(#[$attr:meta])* $vis:vis $name:ident ($($prev:tt)*) $cur:tt $($rest:tt)*
    ) => {
        intrusive_adapter_no_send!(@find_generic
            $(#[$attr])* $vis $name ($($prev)* $cur) $($rest)*
        );
    };
    (@find_if_generic
        $(#[$attr:meta])* $vis:vis $name:ident < $($rest:tt)*
    ) => {
        intrusive_adapter_no_send!(@find_generic
            $(#[$attr])* $vis $name () $($rest)*
        );
    };
    (@find_if_generic
        $(#[$attr:meta])* $vis:vis $name:ident $($rest:tt)*
    ) => {
        intrusive_adapter_no_send!(@impl
            $(#[$attr])* $vis $name () $($rest)*
        );
    };
    ($(#[$attr:meta])* $vis:vis $name:ident $($rest:tt)*) => {
        intrusive_adapter_no_send!(@find_if_generic
            $(#[$attr])* $vis $name $($rest)*
        );
    };
}
