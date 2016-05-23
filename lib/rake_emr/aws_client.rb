module RakeEmr
    class AWSClient
        include Singleton

        def default_options
            {
            "service-role" => "EMR_DefaultRole",
            "ec2-attributes" => {
                #"KeyName"
                #"SubnetId" => "",
                "InstanceProfile" => "EMR_EC2_DefaultRole"
            },
            #"name"
            "enable-debugging" => false,
            #"log-uri"
            "ami-version" => "3.1.0",
            "instance-groups" => [{
                "InstanceGroupType" => "MASTER",
                "InstanceCount" => 1,
                "InstanceType" => "m3.xlarge"
            },{
                "InstanceGroupType" => "CORE",
                "InstanceCount" => 8,
                "BidPrice" => 0.3,
                "InstanceType" => "c3.xlarge"
                }],
            "applications" => {
                "Name" => "GANGLIA"
            },
            "auto-terminate" => false,
            "bootstrap-actions" => [{
                "Path" => "s3://elasticmapreduce/bootstrap-actions/configure-hadoop",
                "Args" => ["-h","dfs.replication=1"]
            }]
        }
        end

        def create_cluster(options={})
            bootstrap_actions = []
            if options.include? "bootstrap-actions"
                bootstrap_actions = options["bootstrap-actions"]
                options.delete "bootstrap-actions"
            end
            param_str = options.map{|k0, v0|
                if v0
                    if v0.is_a? Hash
                        v0 = v0.map{|k1, v1| "#{k1}=#{v1}"}.join(",")
                    elsif v0.is_a? Array
                        v0 = v0.map{|v0_i|
                            if v0_i.is_a? Hash
                                ret = v0_i.map{|v0_i_k, v0_i_v| "#{v0_i_k}=#{v0_i_v}"}.join(",")
                            else
                                raise "format err #{v0_i}, it should be hash"
                            end
                            ret
                        }.join(" ")
                    end
                    "--#{k0} #{v0}"
                else
                    ""
                end
            }.join(" ")
            bootstrap_actions_str = bootstrap_actions.map{|x|
                if not x.include? "Args"
                    x_str = ""
                else
                    x_str = ",Args=["+x["Args"].map{|x| x.inspect}.join(",")+"]"
                end
                "--bootstrap-action Path=#{x["Path"]}#{x_str}"
            }.join(" ")
            cmd = "aws emr create-cluster #{param_str} #{bootstrap_actions_str}"
            cmd = cmd.strip.split.join(" ")
            STDERR.puts "executing: #{cmd}"
            output = `#{cmd}`
            STDERR.puts "result: #{output}"
            js = JSON.parse output
            return js["ClusterId"]
        end

        def terminate_cluster(cluster_id)
            if cluster_id
                cmd = "aws emr terminate-clusters --cluster-ids #{cluster_id}"
                STDERR.puts "executing: #{cmd}"
                out = `#{cmd}`
                STDERR.puts "result:#{out}"
            end
        end

        def describ_cluster(cluster_id)
            cmd = <<CMD
aws emr describe-cluster --cluster-id #{cluster_id}
CMD
            cmd = cmd.strip.split.join(" ")
            STDERR.puts "executing: #{cmd}"
            out = `#{cmd}`
            #STDERR.puts "result:#{out}"
            js = JSON.parse out
            return js
        end

        def wait_for_master_dns(cluster_id, sleep_time: 10)
            while true
                begin
                    res = describ_cluster cluster_id
                    if res['Cluster']['Status']['State'] == 'TERMINATED_WITH_ERRORS' and \
                        res['Cluster']['Status'].include? 'StateChangeReason'  and \
                        res['Cluster']['Status']['StateChangeReason']['Code'] == 'VALIDATION_ERROR'
                        abort '[ERROR] Create cluster failed, terminate with errors'
                    end
                    master_dns = res['Cluster']['MasterPublicDnsName']
                    if master_dns and master_dns.size > 0
                        return master_dns
                    end
                rescue Exception => e
                    STDERR.puts e
                end
                sleep sleep_time
            end
        end

        def wait_for_master_hadoop_ready master_dns, ssl_ca_file, sleep_time: 10
            while true
                cmd = "ssh -i #{ssl_ca_file} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null hadoop@#{master_dns} hadoop fs -ls /tmp/"
                STDERR.puts "executing: #{cmd}"
                out = `#{cmd}`
                if out.include? "/tmp/hadoop-yarn"
                    return true
                end
                sleep sleep_time
            end
        end
    end
end