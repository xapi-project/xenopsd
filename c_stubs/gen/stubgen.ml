let c_headers = "#include <xenctrl.h>"
let () =
  Format.fprintf Format.std_formatter "%s@\n" c_headers;
  Cstubs_structs.write_c Format.std_formatter (module Bindings_structs_lib.Bindings_structs.Make)
