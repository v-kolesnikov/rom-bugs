#!/usr/bin/env ruby

require 'bundler/inline'

gemfile(true) do
  gem 'dry-struct', '~> 0.3.1'
  gem 'rom-mongo', github: 'rom-rb/rom-mongo', branch: :master
  gem 'rspec'
end

require 'rspec'
require 'rspec/autorun'

require 'rom-mongo'

RSpec.configure do |config|
  config.color = true
  config.formatter = :documentation
end

RSpec.describe ROM::Mongo do
  let(:conn) { Mongo::Client.new('mongodb://0.0.0.0:27017/rom_mongo') }
  let(:conf) { ROM::Configuration.new(:mongo, conn) }
  let(:container) { ROM.container(conf) }

  before do
    conn[:users].drop

    conf.relation(:users) do
      schema(:users) do
        attribute :_id,   ROM::Types.Definition(BSON::ObjectId)
        attribute :name,  ROM::Types::String
        attribute :email, ROM::Types::String
      end
    end
  end

  it { expect { container }.not_to raise_error }

  describe 'relation with association' do
    before do
      conf.relation(:tasks) do
        schema(:tasks) do
          attribute :_id,     ROM::Types.Definition(BSON::ObjectId)
          attribute :user_id, ROM::Types::Int

          associations do
            belongs_to :user, foreign_key: :u_id
          end
        end
      end
    end

    it { expect { container }.not_to raise_error }
  end
end
