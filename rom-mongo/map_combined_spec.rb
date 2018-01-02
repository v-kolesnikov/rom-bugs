#!/usr/bin/env ruby

require 'bundler/inline'

gemfile(true) do
  gem 'dry-struct', '~> 0.3.1'
  gem 'pg'
  gem 'pry-byebug'
  gem 'rom-sql'
  gem 'rom-mongo', github: 'rom-rb/rom-mongo', branch: :master
  gem 'rspec'
end

require 'pry'

require 'rom-sql'
require 'rom-mongo'
require 'rom/transformer'

require 'rspec'
require 'rspec/autorun'

RSpec.configure do |config|
  config.color = true
  config.formatter = :documentation
end

RSpec.describe ROM::Mongo do
  let(:postgres) { Sequel.connect('postgres://0.0.0.0/rom_sql') }
  let(:mongo_db) { Mongo::Client.new('mongodb://0.0.0.0:27017/rom_mongo') }

  let(:conf) do
    ROM::Configuration.new(
      postgres: [:sql, postgres],
      mongo_db: [:mongo, mongo_db.with(logger: logger)]
    )
  end

  let(:container) { ROM.container(conf) }

  let(:logger) { ::Logger.new('log.log') }

  # Context:
  # SQL DB #1
  #   Users :id, :name
  #   Tasks :id, :name
  #   Tags  :id, :name, :task_id
  #
  # MongoDB
  #   UsersTasks :_id, :user_id, :task_id, :points (user_id_task_id is not unique)
  #
  # Expected relation
  #
  # Users
  # |->> UserTasks (aggregation: group by (user_id, task_id), sum of points)
  #      |-<> Task
  #           |->> Tags
  #
  # A |->> B - A has many B
  # A |-<> B - A belongs to B
  #
  # Known issues:
  #  - Mapper doesn't symbolize keys
  #  - Registered mapper doesn't work when `mapper.build.call(relation)` - works
  #  - 'INNER JOIN' statement on tasks.combine(:tags)

  before do
    postgres.drop_table?(:users)
    postgres.drop_table?(:tasks)
    postgres.drop_table?(:tags)
    mongo_db[:user_tasks].drop

    postgres.create_table :users do
      primary_key :id
      column :name, String
    end

    postgres.create_table :tasks do
      primary_key :id
      column :name, String
    end

    postgres.create_table :tags do
      primary_key :id
      column :name, String
      column :task_id, Integer
    end

    conf.relation(:users, gateway: :postgres) do
      schema(:users, infer: true) do
      end
    end

    conf.relation(:tasks, gateway: :postgres) do
      schema(:tasks, infer: true) do
        associations do
          has_many :tags
        end
      end

      def for_tags(tags)
        where(id: tags.pluck(:task_id))
      end

      def for_user_tasks(user_tasks)
        where(id: user_tasks.pluck('task_id'))
      end

      def for_aggregated_user_tasks(user_tasks)
        where(id: user_tasks.map { |user_task| user_task['_id']['task_id'] })
      end
    end

    conf.relation(:tags, gateway: :postgres) do
      schema(:tags, infer: true) do
        associations do
          belongs_to :task
        end
      end

      def for_tasks(tasks)
        where(task_id: tasks.pluck(:id))
      end
    end

    conf.relation(:user_tasks, gateway: :mongo_db, adapter: :mongo) do
      schema(:user_tasks) do
        attribute :_id,     ROM::Types.Definition(BSON::ObjectId)
        attribute :user_id, ROM::Types::Int
        attribute :task_id, ROM::Types::Int
        attribute :points,  ROM::Types::Int
      end

      def for_users(users)
        where(user_id: { '$in': users.pluck(:id) })
      end

      def sum_of_user_task_points_by_users(users)
        query = [
          {
            '$match' => {
              'user_id' => { '$in' => users.pluck(:id) }
            }
          },

          {
            '$group' => {
              '_id' => {
                'user_id' => '$user_id',
                'task_id' => '$task_id'
              },
              'points' => { '$sum' => '$points' }
            }
          },

          {
            '$project' => {
              'user_id' => '$_id.user_id',
              'task_id' => '$_id.task_id',
              'points'  => '$points'
            }
          },

          {
            '$sort' => { 'user_id' => 1, 'task_id' => 1 }
          }
        ]
        new(dataset.collection.find.aggregate(query))
      end
    end

    conf.register_mapper(mapper)
    conf.gateways[:postgres].use_logger logger

    postgres[:users].insert name: 'Jane'
    postgres[:users].insert name: 'Jonh'
    postgres[:users].insert name: 'Joe'

    postgres[:tasks].insert name: 'task #1'
    postgres[:tasks].insert name: 'task #2'
    postgres[:tasks].insert name: 'task #3'

    postgres[:tags].insert name: 'tag #1', task_id: 1
    postgres[:tags].insert name: 'tag #2', task_id: 2
    postgres[:tags].insert name: 'tag #3', task_id: 3

    user_tasks.insert user_id: 1, task_id: 1, points: 10
    user_tasks.insert user_id: 1, task_id: 1, points: 20

    user_tasks.insert user_id: 2, task_id: 2, points: 30
    user_tasks.insert user_id: 2, task_id: 2, points: 40

    user_tasks.insert user_id: 3, task_id: 3, points: 50
    user_tasks.insert user_id: 3, task_id: 3, points: 60
  end

  let(:users) { container.relations[:users] }
  let(:tasks) { container.relations[:tasks] }
  let(:tags)  { container.relations[:tags] }
  let(:user_tasks) { container.relations[:user_tasks] }

  describe 'custom mapper based on ROM::Mapper' do
    let(:relation) do
      users
        .with(auto_map: false)
        .combine_with(
          user_tasks.with(auto_map: false).sum_of_user_task_points_by_users.combine_with(
            tasks.with(auto_map: false).for_aggregated_user_tasks.combine_with( # FIXME: Is it possible to use `combine(:tags)` here?
              tags.with(auto_map: false).for_tasks
            )
          )
        )
    end

    let(:mapper) do
      Class.new(ROM::Mapper) do
        relation :users

        register_as :entity

        attribute :id
        attribute :name

        combine :user_tasks, on: { id: 'user_id' } do
          attribute :user_id, from: 'user_id'
          attribute :task_id, from: 'task_id'
          attribute :points,  from: 'points'

          exclude '_id'

          # FIXME: tasks must be wrapped into user_tasks
          combine :tasks, on: { 'task_id' => :id } do
            attribute :id
            attribute :name

            combine :tags, on: { id: :task_id } do
              attribute :id
              attribute :name
            end
          end
        end
      end
    end

    let(:expected_data) do
      [{ id: 1, name: 'Jane', user_tasks: [{ user_id: 1, task_id: 1, points: 30,  task: { id: 1, name: 'task #1', tags: [{ id: 1, name: 'tag #1', task_id: 1 }] } }] },
       { id: 2, name: 'Jonh', user_tasks: [{ user_id: 2, task_id: 2, points: 70,  task: { id: 2, name: 'task #2', tags: [{ id: 2, name: 'tag #2', task_id: 2 }] } }] },
       { id: 3, name: 'Joe',  user_tasks: [{ user_id: 3, task_id: 3, points: 110, task: { id: 3, name: 'task #3', tags: [{ id: 3, name: 'tag #3', task_id: 3 }] } }] }]
    end

    subject(:data) { mapper.build.(relation) }

    it do
      expect(data).to eq(expected_data)
    end

    context 'with registered mapper' do
      subject(:data) { relation.map_with(:entity) }

      it { expect { subject.to_a }.not_to raise_error }
      it { expect(data).to match_array(expected_data) }
    end
  end
end
