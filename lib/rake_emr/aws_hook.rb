require "rake_exception_hook"
module RakeEmr
    RakeExceptionHook.start do
        webhdfs_port = 9101
        if RakeEmr.need_to_create_cluster? and RakeEmr.init_param
            STDERR.puts "[LOG] starting cluster"
            raise "KeyName is not setted" if not RakeEmr.init_param["ec2-attributes"] or not RakeEmr.init_param["ec2-attributes"]["KeyName"]
            raise "name is not setted" if not RakeEmr.init_param["name"]
            raise "log-uri is not setted" if not RakeEmr.init_param["log-uri"]
            raise "ssl_ca_file is not setted" if not RakeEmr.ssl_ca_file
            client = AWSClient.instance
            cluster_id = client.create_cluster RakeEmr.init_param
            STDERR.puts "[LOG] waiting for master init"
            master_dns = client.wait_for_master_dns cluster_id
            STDERR.puts "#### cluster_id: #{cluster_id}"
            STDERR.puts "#### master_dns: #{master_dns}"
            STDERR.puts "[LOG] waiting for hadoop init"
            client.wait_for_master_hadoop_ready master_dns, RakeEmr.ssl_ca_file
            RakeEmr.set_cluster cluster_id, master_dns
        end
        if RakeEmr.on_aws?
            RakeEmr.set_webhdfs_port webhdfs_port
            STDERR.puts "#### connecting webhdfs : #{RakeEmr.master_name}: #{RakeEmr.webhdfs_port}"
            WebHDFS::FileUtils.set_server(RakeEmr.master_name, RakeEmr.webhdfs_port)
            dir_occupied = %w{bin conf etc hive libexec pig Cascading-2.5-SDK contrib hadoop-examples.jar lib mahout sbin share}
            RakeEmr.script_dirs.each do |d|
                raise "Dir name #{d} is occupied by aws." if dir_occupied.include? d.gsub(/\/+$/, "").split()[-1]
                cmd = "scp -i #{RakeEmr.ssl_ca_file} -o StrictHostKeyChecking=no -r #{d} hadoop@#{RakeEmr.master_name}:~/"
                STDERR.puts "executing:#{cmd}"
                `#{cmd}`
            end
            # STDERR.puts "#### opening proxy port for webhdfs ####"
            # cmd = "ssh -i #{RakeEmr.ssl_ca_file} -o StrictHostKeyChecking=no hadoop@#{RakeEmr.master_name} -D #{RakeEmr.local_proxy_socket_port}"
            # STDERR.puts "executing:#{cmd}"
            # Process.fork { system cmd } # run ssh proxy in background
        end
        if RakeEmr.init_task
            RakeEmr.init_task.call
        end
    end

    RakeExceptionHook.finish do
        STDERR.puts "[LOG] rake task was finished"
        if RakeEmr.finish_task
            RakeEmr.finish_task.call
        end
        if RakeEmr.need_to_destroy_cluster? and RakeEmr.on_aws?
            client = AWSClient.instance
            client.terminate_cluster RakeEmr.cluster_id
            RakeEmr.reset_cluster
        end
    end

    RakeExceptionHook.except do |e|
        STDERR.puts "[ERR] rake task failed"
        STDERR.puts e
        if RakeEmr.except_task
            RakeEmr.except_task.call
        end
        if RakeEmr.need_to_destroy_cluster? and RakeEmr.on_aws?
            client = AWSClient.instance
            client.terminate_cluster RakeEmr.cluster_id
            RakeEmr.reset_cluster
        end
    end
end