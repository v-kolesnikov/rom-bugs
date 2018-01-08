#!/usr/bin/env ruby

require 'bundler/inline'

gemfile(true) do
  gem 'dry-struct', '~> 0.3.1'
  gem 'dry-types'
  gem 'pg'
  gem 'pry-byebug'
  gem 'rom-sql', path: '~/projects/forks/rom-rb/rom-sql'
  gem 'rspec'
end

require 'dry-types'
require 'rom-sql'
require 'rom/mapper'

require 'pry'
require 'rspec'
require 'rspec/autorun'

LOGGER = ::Logger.new('log.log')

module Types
  include Dry::Types.module
end

RSpec.configure do |config|
  config.color = true
  config.formatter = :documentation
end

RSpec.describe ROM::Mapper do
  # NOTE: It is a important option: I have two different gateways.
  let(:db1) { Sequel.connect('postgres://0.0.0.0/db_1') }
  let(:db2) { Sequel.connect('postgres://0.0.0.0/db_2') }

  let(:conf) do
    ROM::Configuration.new(
      db1: [:sql, db1],
      db2: [:sql, db2],
      db3: [:memory]
    )
  end

  let(:container) { ROM.container(conf) }

  before do
    db1.drop_table?(:tasks)
    db2.drop_table?(:users)

    db1.create_table :tasks do
      primary_key :id
      column :name, String
      column :user_id, Integer
    end

    db2.create_table :users do
      primary_key :id
      column :name, String
    end

    conf.relation(:tasks, gateway: :db1) do
      schema(:tasks, infer: true) do
        associations do
          belongs_to :user
        end
      end
    end

    conf.relation(:users, gateway: :db2) do
      schema(:users, infer: true) do
        associations do
          has_many :tasks
        end
      end

      def for_tasks(tasks)
        where(id: tasks.pluck(:user_id).uniq)
      end
    end

    conf.relation(:emails, gateway: :db3, adapter: :memory) do
      schema(:emails) do
        attribute :id,      Types::Strict::Int
        attribute :user_id, Types::Strict::Int
        attribute :email,   Types::Strict::String
      end

      def for_users(users)
        restrict(user_id: users.pluck(:id).uniq)
      end
    end

    conf.register_mapper(mapper_class)

    conf.gateways[:db1].use_logger LOGGER
    conf.gateways[:db2].use_logger LOGGER
    conf.gateways[:db3].use_logger LOGGER

    jane_id = db2[:users].insert name: 'Jane'
    joe_id  = db2[:users].insert name: 'Joe'

    db1[:tasks].insert name: 'task #1', user_id: jane_id
    db1[:tasks].insert name: 'task #2', user_id: jane_id
    db1[:tasks].insert name: 'task #3', user_id: joe_id
    db1[:tasks].insert name: 'task #4', user_id: joe_id

    emails_dataset = conf.gateways[:db3].dataset(:emails)
    emails_dataset.insert id: 1, user_id: 1, email: 'jane@gmail.com'
    emails_dataset.insert id: 2, user_id: 1, email: 'jane@github.com'
    emails_dataset.insert id: 3, user_id: 2, email: 'joe@gmail.com'
    emails_dataset.insert id: 4, user_id: 2, email: 'joe@github.com'
  end

  let(:users)  { container.relations.users }
  let(:tasks)  { container.relations.tasks }
  let(:emails) { container.relations.emails }

  describe 'map tasks with user and emails' do
    let(:relation) do
      tasks
        .with(auto_map: false)
        .combine_with(
          users.with(auto_map: false).for_tasks.combine_with(
            emails.with(auto_map: false).for_users
          )
        )
    end

    let(:mapper_class) do
      Class.new(ROM::Mapper) do
        relation :tasks
        register_as :entity

        # There is `combine` here, but must be `wrap` instead.
        combine :users, on: { user_id: :id } do
          combine :emails, on: { id: :user_id } do
            exclude :user_id
          end
        end
      end
    end

    let(:expected_data) do
      [
        { name: 'task #1', user_id: 1,
          user: { id: 1, name: 'Jane',
                  emails: [{ id: 1, email: 'jane@gmail.com' },
                           { id: 2, email: 'jane@github.com' }] } },

        { name: 'task #2', user_id: 1,
          user: { id: 1, name: 'Jane',
                  emails: [{ id: 1, email: 'jane@gmail.com' },
                           { id: 2, email: 'jane@github.com' }] } },

        { name: 'task #3', user_id: 2,
          user: { id: 2, name: 'Joe',
                  emails: [{ id: 3, email: 'joe@gmail.com' },
                           { id: 4, email: 'joe@github.com' }] } },

        { name: 'task #4', user_id: 2,
          user: { id: 2, name: 'Joe',
                  emails: [{ id: 3, email: 'joe@gmail.com' },
                           { id: 4, email: 'joe@github.com' }] } }
      ]
    end

    subject(:data) { relation.>>(mapper_class.build) }

    it 'allows to use `.>>(mapper_class.build)` and `.map_with(:mapper)`' do
      expect(relation.>>(mapper_class.build).eql?(relation.map_with(:entity)))
      expect { relation.>>(mapper_class.build).to_a }.not_to raise_error
      # FIXME: This is failed
      expect { relation.map_with(:entity).to_a }.not_to raise_error
    end

    it 'returns expected data' do
      expect(data.to_a).to eq expected_data
    end
  end
end
