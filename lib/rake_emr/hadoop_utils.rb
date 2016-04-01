require "set"
module FileUtils
    def rsh (cmd, &block)
        if RakeEmr.on_aws?
            raise "AWS is not initialized" if RakeEmr.initialized?
            raise "ssl ca file is not setted" if not RakeEmr.ssl_ca_file
            cmd = "ssh -i #{RakeEmr.ssl_ca_file} -o StrictHostKeyChecking=no hadoop@#{RakeEmr.master_name} bash #{cmd}"
        end
        sh cmd, &block
    end

    def distcp(src, dst)
        rsh "hadoop distcp #{src} #{dst}"
    end

    def adistcp(src, dst)
        if RakeEmr.on_aws?
            rsh "hadoop distcp #{src} #{dst}"
        else
            STDERR.puts "[aws] hadoop distcp #{src} #{dst}"
        end
    end

    def pig(script, param = {})
        param_str = param.map{|k,v| "-p #{k}=#{v}"}.join(" ")
        rsh "pig #{srcipt} #{param_str}"
    end

    def streaming(options = {})
        default_options = {
            "Dmapreduce.job.name" => "rake_aws_hadoop_anonymous",
            "Dmapreduce.map.tasks" => 1,
            "Dmapreduce.reduce.tasks" => 1,
            "Dmapreduce.map.tasks.speculative.execution" => false,
            "Dmapreduce.reduce.tasks.speculative.execution" => false,
            "Dstream.non.zero.exit.is.failure" => false,
            "numReduceTasks" => 100
        }
        options = Hash[options.map{|k,str| [k.to_s,v] } ]

        if not options.include? "mapper" and not options.include? "reducer"
            raise "No mapper and reducer for streamming"
        end
        if not options.include? "input"
            raise "No input for streamming"
        end

        if not options.include? "output"
            raise "No output for streaming"
        end

        if not options.include? "files"
            raise "files is not setted for streaming"
        end
        if not options["files"].is_a? String
            raise "files parameter error" if not options.respond_to? :each
            options["files"] = options["files"].map{|x| x.to_s}.join ","
        end

        default_options.update(options)
        option_str = default_options.map do |k,v|
            if k.start_with? "D"
                "-#{k}=#{v}"
            else
                "-#{k} #{v}"
            end
        end.join(" ")
        env_streaming = ENV["HADOOP_STEAMMING_LIB"]
        streaming_lib = if env_streaming then env_streaming else "/home/hadoop/contrib/streaming/hadoop-streaming.jar" end
        cmd = "hadoop jar #{streaming_lib} #{option_str}"
        rsh cmd
    end
end
