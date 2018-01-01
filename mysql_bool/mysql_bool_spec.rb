#!/usr/bin/env ruby

require 'bundler/inline'

# gemfile(true) do
#   gem 'rom-sql'
#   gem 'rspec'
#   gem 'mysql2'
#   gem 'pry-byebug'
# end

require 'pry'
require 'rspec'
require 'rspec/autorun'

require 'dry-types'
require 'mysql2'
require 'rom-core'
require 'rom-sql'

ROM::SQL.load_extensions :mysql

RSpec.configure do |config|
  config.color = true
  config.formatter = :documentation
end

RSpec.describe 'Select boolean functions' do
  let(:uri) { 'mysql2://root@0.0.0.0/bool_example' }
  let(:conn) { Sequel.connect(uri) }
  let(:conf) { ROM::Configuration.new(:sql, conn) }
  let(:container) { ROM.container(conf) }

  before do
    conn.create_table :samples do
      column :flag, 'boolean'
    end

    conf.relation(:samples) do
      schema do
        attribute :flag, ROM::SQL::Types::Strict::Bool.optional
      end
    end
  end

  after :each do
    conn.drop_table?(:samples)
  end

  let(:relation) { container.relations.samples }

  subject(:data) do
    relation.select { bool::bit_and(flag).as(:flag) }.to_a
  end

  context 'when any flags is `false`' do
    before do
      container.relations.samples.command(:create).(flag: true)
      container.relations.samples.command(:create).(flag: false)
    end

    it { expect(relation.to_a).to eq [{ flag: true }, { flag: false }] }
    it { expect(data).to eq [{ flag: false }] }
  end

  context 'when all flags are `true`' do
    before do
      container.relations.samples.command(:create).(flag: true)
      container.relations.samples.command(:create).(flag: true)
    end

    it { expect(relation.to_a).to eq [{ flag: true }, { flag: true }] }
    it { expect(data).to eq [{ flag: true }] }
  end
end
