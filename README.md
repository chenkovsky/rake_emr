# RakeEmr

Welcome to your new gem! In this directory, you'll find the files you need to be able to package up your Ruby library into a gem. Put your Ruby code in the file `lib/rake_emr`. To experiment with that code, run `bin/console` for an interactive prompt.

TODO: Delete this and the text above, and describe your gem

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
RakeEmr.default_config("chenkovsky.chen", "ime_user_profile", "s3://user.cootek/chenkovsky.chen/log")
RakeEmr.set_ssl_ca_file "/home/chenkovsky.chen/.ssh/chenkovsky.chen.pem"
RakeEmr.script_dirs << "scripts"

task :A do
    rsh "hadoop fs ls .."
    adistcp "s3://....", "hdfs://....."
end
```

after config the parameters. execute

```bash
rake A
```

this library will take care of cluster initialization, and deinitialization.
if you comment configure,

```ruby
#RakeEmr.default_config("chenkovsky.chen", "ime_user_profile", "s3://user.cootek/chenkovsky.chen/log")
```

then you can run it on local hadoop.

## Contributing

1. Fork it ( https://github.com/chenkovsky/rake_emr/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
