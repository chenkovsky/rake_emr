require "webhdfs/client_v1"

module WebHDFS
  class ClientV1
    # since on aws it's difficult to call webhdfs api.
    # use curl.
    alias old_request request
    def request(host, port, method, path, op = nil, params = {}, payload = nil, header = nil, _retries = 0)
      if RakeEmr.on_aws?
        raise "Op #{op} not supported" if not ["MKDIRS", "GETFILESTATUS"].include? op
        path = Addressable::URI.escape(path) # make path safe for transmission via HTTP
        request_path = if op
          params["user.name"] = "hadoop"
          build_path(path, op, params)
        else
          path
        end
        message = nil
        begin
          message = send_request(method, request_path, payload, header)
        rescue => e
          raise WebHDFS::ServerError, "Failed to connect to host #{host}:#{port}, #{e.message}"
        end
        js = JSON.parse message
        if js["RemoteException"]
          case js["RemoteException"]["exception"]
          when "FileNotFoundException"
            raise WebHDFS::FileNotFoundError, message
          when "SecurityException"
            raise WebHDFS::SecurityError, message
          else
            raise WebHDFS::ServerError, message
          end
        end
        return message
      else
        return old_request(host, port, method, path, op, params, payload, header, _retries)
      end
    end

    alias old_check_success_json check_success_json
    def check_success_json(res, attr=nil)
      if RakeEmr.on_aws?
        (attr.nil? or JSON.parse(res)[attr])
      else
        old_check_success_json(res, attr)
      end
    end

    def send_request(method, request_path, payload, header)
      header_str = nil
      if header
        header_str = "--header #{header.inspect}"
      end
      url = "http://#{@host}:#{@port}#{request_path}"
      cmd = "ssh -i #{RakeEmr.ssl_ca_file} -o StrictHostKeyChecking=no hadoop@#{RakeEmr.master_name} 'curl -X #{method} #{header_str} #{url.inspect}'"
      puts "executing: #{cmd}"
      `#{cmd}`
    end
  end
end
