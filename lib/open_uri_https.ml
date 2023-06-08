
(* This code is usually in xcp-idl but we introduced a local copy here
   to support https, which has a dependency on stunnel and would create
   a circular dependency. *)

let https_port = 443

let with_open_uri uri f =
  let finally = Xapi_stdext_pervasives.Pervasiveext.finally in
  match Uri.scheme uri with
  | Some "http" -> (
    match (Uri.host uri, Uri.port uri) with
    | Some host, Some port ->
        Open_uri.open_tcp f host port
    | Some host, None ->
        Open_uri.open_tcp f host 80
    | _, _ ->
        failwith
          (Printf.sprintf "Failed to parse host and port from URI: %s"
             (Uri.to_string uri)
          )
  )
  | Some "https" -> (
    let process (s : Stunnel.t) =
      finally
        (fun () -> f Safe_resources.Unixfd.(!(s.Stunnel.fd)))
        (fun () -> Stunnel.disconnect s)
    in
    match (Uri.host uri, Uri.port uri) with
    | Some host, Some port ->
        Stunnel.with_connect host port process
    | Some host, None ->
        Stunnel.with_connect host https_port process
    | _, _ ->
        failwith
          (Printf.sprintf "Failed to parse host and port from URI: %s"
             (Uri.to_string uri)
          )
  )
  | Some "file" ->
      let filename = Uri.path_and_query uri in
      let sockaddr = Unix.ADDR_UNIX filename in
      let s = Unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
      finally
        (fun () -> Unix.connect s sockaddr ; Open_uri.handle_socket f s)
        (fun () -> Unix.close s)
  | Some x ->
      failwith (Printf.sprintf "Unsupported URI scheme: %s" x)
  | None ->
      failwith (Printf.sprintf "Failed to parse URI: %s" (Uri.to_string uri))


