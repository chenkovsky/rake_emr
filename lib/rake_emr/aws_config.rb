require "set"
require 'webhdfs/fileutils'
module RakeEmr
  # 初始化cluster的参数
  @@except_task = nil
  @@finish_task = nil
  @@init_task = nil
  @@init_param = nil

  @@cluster_id = nil
  @@master_name = nil
  @@on_already_created_cluster = false
  @@keep_cluster_open = false

  @@webhdfs_port=50070
  @@webhdfs_username = nil
  def self.set_cluster_init_param(options={})
    @@init_param = options
  end

  def self.init_param
    @@init_param
  end

  def self.set_ssl_ca_file(pem_path)
    @@ssl_ca_file = pem_path
  end

  def self.ssl_ca_file
    @@ssl_ca_file
  end

  def self.set_cluster cluster_id, master_name
    @@cluster_id = cluster_id
    @@master_name = master_name
    WebHDFS::FileUtils.set_server(master_name, @@webhdfs_port, @@webhdfs_username, @@webhdfs_username)
  end

  def self.on_cluster cluster_id, master_name
    set_cluster cluster_id, master_name
    @@on_already_created_cluster = true
  end

  def self.cluster_id
    @@cluster_id
  end

  def self.master_name
    @@master_name
  end

  def self.reset_cluster
    @@cluster_id = nil
    @@master_name = nil
  end

  @@script_dirs = Set.new
  def self.script_dirs
    @@script_dirs
  end

  def self.when_init &block
    @@init_task = block
  end

  def self.when_finish &block
    @@finish_task = block
  end

  def self.when_except &block
    @@except_task = block
  end

  def self.init_task
    @@init_task
  end

  def self.finish_task
    @@finish_task
  end

  def self.except_task
    @@except_task
  end

  def self.on_aws?
    not @@init_param.nil?
  end

  def self.initialized?
    not @@master_name.nil?
  end

  def self.need_to_create_cluster?
    not @@on_already_created_cluster
  end

  def self.need_to_destroy_cluster?
    not @@on_already_created_cluster and not @@keep_cluster_open
  end

  def self.keep_cluster_open
    @@keep_cluster_open = true
  end
end
