# RakeEmr
*developping*
make rake run on aws emr.

when running tasks on emr. you have two choices.

One is offical aws add step, but this cannot be applied to complex tasks. 

The other is login to the master of cluster, copy your scripts to master, and run tasks on master. For this choice, you have to take care of cluster status. 

cluster initialization takes a lot of time and money. We want to test **whole** task flow on local hadoop, and run it on aws without changing any code.

this library takes care of all things for you.


## Installation

Add this line to your application's Gemfile:

```ruby
gem 'rake_emr'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install rake_emr

## Usage

```ruby
if not ENV["on_local"]
    RakeEmr.default_config("chenkovsky.chen", "ime_user_profile", "s3://chenkovsky.chen/log")
end
RakeEmr.set_ssl_ca_file "/home/chenkovsky.chen/.ssh/chenkovsky.chen.pem"
RakeEmr.script_dirs << "scripts"

task :A do
    rsh "hadoop fs ls .."
    adistcp "s3://....", "hdfs://....."
end
```

after config the parameters. execute

```bash
rake A # run on aws
rake A on_local=true # run on local hdfs
```




## Contributing

1. Fork it ( https://github.com/chenkovsky/rake_emr/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
