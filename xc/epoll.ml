open Unix

type event =
  | EPOLLIN
  | EPOLLOUT
  | EPOLLRDHUP
  | EPOLLPRI
  | EPOLLERR
  | EPOLLHUP
  | EPOLLET
  | EPOLLONESHOT

type operation =
  | EPOLL_CTL_ADD
  | EPOLL_CTL_MOD
  | EPOLL_CTL_DEL

type flag = EPOLL_CLOEXEC

external create1 : flag list -> file_descr = "caml_epoll_create1"

external ctl : file_descr -> operation -> file_descr -> event list -> unit = "caml_epoll_ctl"

external wait : file_descr -> int -> int -> (event list * file_descr) list = "caml_epoll_wait"
