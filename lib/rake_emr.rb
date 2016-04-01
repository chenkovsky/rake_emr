require "rake_emr/version"
require "rake_emr/aws_config"
require "rake_emr/aws_hook"
require "rake_emr/hadoop_utils"
require "rake_emr/aws_client"
module RakeEmr
  def self.default_config(key_name, cluster_name, log_path, bootstrap_actions: [])
    conf = AWSClient.instance.default_options
    conf["ec2-attributes"]["KeyName"] = key_name
    conf["name"] = cluster_name
    conf["log-uri"] = log_path
    conf["bootstrap-actions"] += bootstrap_actions
    @@webhdfs_username = key_name
    set_cluster_init_param conf
  end
end
