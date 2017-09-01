#include <stdint.h>
#include <sys/epoll.h>

#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/fail.h>
#include <caml/threads.h>
#include <caml/unixsupport.h>
#include <caml/custom.h>

#define FLAGS_LEN(flags) sizeof(flags) / sizeof(flags[0])

static int epoll_events[] = {
    EPOLLIN,
    EPOLLOUT,
    EPOLLRDHUP,
    EPOLLPRI,
    EPOLLERR,
    EPOLLHUP,
    EPOLLET,
    EPOLLONESHOT
};

static int epoll_operations[] = {
    EPOLL_CTL_ADD,
    EPOLL_CTL_MOD,
    EPOLL_CTL_DEL
};

static int epoll_flags[] = { EPOLL_CLOEXEC };

value
caml_cons(value list, value val) {
    CAMLparam2(list, val);
    CAMLlocal1(list2);

    list2 = caml_alloc(2, 0);
    Store_field(list2, 0, val);
    Store_field(list2, 1, list);

    CAMLreturn(list2);
}

value
caml_unpack_flags(int pack, int *flags, int flags_len) {
    CAMLparam0();
    CAMLlocal1(flags_list);
    int i;

    flags_list = Val_emptylist;
    for (i = 0; i < flags_len; i++) {
        if (pack & flags[i]) {
            flags_list = caml_cons(flags_list, Val_int(i));
        }
    }

    CAMLreturn(flags_list);
}

CAMLprim value
caml_epoll_create1(value val_flags) {
    CAMLparam1(val_flags);
    CAMLlocal1(val_res);
    int fd, flags = caml_convert_flag_list(val_flags, epoll_flags);

    if ((fd = epoll_create1(flags)) == -1) {
        uerror("epoll_create1", Nothing);
    }

    val_res = Val_int(fd);

    CAMLreturn(val_res);
}

CAMLprim value
caml_epoll_ctl(value val_epfd, value val_op, value val_fd, value val_events) {
    CAMLparam4(val_epfd, val_op, val_fd, val_events);
    int op = epoll_operations[Int_val(val_op)];
    struct epoll_event event = {.events = caml_convert_flag_list(val_events, epoll_events),
                                .data.fd = Int_val(val_fd) };

    if (epoll_ctl(Int_val(val_epfd), op, Int_val(val_fd), &event) == -1) {
        uerror("epoll_ctl", Nothing);
    }

    CAMLreturn(Val_unit);
}

CAMLprim value
caml_epoll_wait(value val_epfd, value val_maxevents, value val_timeout) {
    CAMLparam3(val_epfd, val_maxevents, val_timeout);
    CAMLlocal2(events_list, elem);
    struct epoll_event events[Int_val(val_maxevents)];
    int num_events, i;

    caml_enter_blocking_section();
    num_events = epoll_wait(Int_val(val_epfd), events, Int_val(val_maxevents),
                            Int_val(val_timeout));
    caml_leave_blocking_section();

    if (num_events == -1) {
        uerror("epoll_wait", Nothing);
    }

    events_list = Val_emptylist;
    for (i = 0; i < num_events; i++) {
        elem = caml_alloc_tuple(2);
        Store_field(elem, 0,
                    caml_unpack_flags(events[i].events, epoll_events,
                                      FLAGS_LEN(epoll_events)));
        Store_field(elem, 1, Val_int(events[i].data.fd));
        events_list = caml_cons(events_list, elem);
    }

    CAMLreturn(events_list);
}
