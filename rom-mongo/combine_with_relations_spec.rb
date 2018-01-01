#!/usr/bin/env ruby

require 'bundler/inline'

gemfile(true) do
  gem 'dry-struct', '~> 0.3.1'
  gem 'pg'
  gem 'pry-byebug'
  gem 'rom-sql'
  gem 'rom-mongo', github: 'rom-rb/rom-mongo', branch: :master
  gem 'rom-sql'
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
  # Goal:
  # - Be able to use custom mapper for cross-adapter relations only:
  # users->>users_tasks and users_tasks-<>task, but not for tasks->>tags.
  # - Minimize runtime data processing
  #
  # FIXME:
  # - Explore and fix 'INNER JOIN' statement on tags relation

  before do
    logger.debug("\n\nNext build:\n")

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
        where(id: tasks.pluck(:id))
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
            '$sort' => { 'user_id' => 1, 'task_id' => 1 }
          }
        ]
        new(dataset.collection.find.aggregate(query))
      end
    end

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

    container.gateways[:postgres].use_logger logger
  end

  let(:users) { container.relations[:users] }
  let(:tasks) { container.relations[:tasks] }
  let(:tags) { container.relations[:tags] }
  let(:user_tasks) { container.relations[:user_tasks] }

  let(:map_symbolizer) do
    Class.new(ROM::Transformer) do
      map_array do
        deep_symbolize_keys
      end
    end.new
  end

  let(:map_users_with_tasks_and_tags) do
    lambda do |(users, ((user_tasks, ((tasks, (tags))))))|
      users.map do |user|
        user[:user_tasks] = user_tasks.select do |user_task|
          user_task[:_id][:user_id] == user[:id]
        end

        user[:user_tasks] = user[:user_tasks].map do |user_task|
          user_task[:task] = tasks.find { |task| task[:id] == user_task[:_id][:task_id] }
          user_task[:task][:tags] = tags.select { |tag| tag[:task_id] == user_task[:task][:id] }
          user_task
        end

        user
      end
    end
  end

  subject(:relation) do
    users
      .with(auto_map: false)
      .combine_with(
        user_tasks.with(auto_map: false).sum_of_user_task_points_by_users.combine_with(
          tasks.with(auto_map: false).combine(:tags).for_aggregated_user_tasks
        )
      )
      .>>(map_users_with_tasks_and_tags)
      .>>(map_symbolizer) # FIXME: use symbolizer for user_tasks relation only
  end

  it 'aggregates user tasks points' do
    relation = user_tasks.sum_of_user_task_points_by_users(users)

    expect(relation.>>(map_symbolizer).to_a)
      .to eq([{ _id: { user_id: 3, task_id: 3 }, points: 110 },
              { _id: { user_id: 2, task_id: 2 }, points: 70 },
              { _id: { user_id: 1, task_id: 1 }, points: 30 }])
  end

  let(:expected_data) do
    [{ id: 1, name: 'Jane', user_tasks: [{ _id: { user_id: 1, task_id: 1 }, points: 30, task: { id: 1, name: 'task #1', tags: [{ id: 1, name: 'tag #1', task_id: 1 }] } }] },
     { id: 2, name: 'Jonh', user_tasks: [{ _id: { user_id: 2, task_id: 2 }, points: 70, task: { id: 2, name: 'task #2', tags: [{ id: 2, name: 'tag #2', task_id: 2 }] } }] },
     { id: 3, name: 'Joe', user_tasks: [{ _id: { user_id: 3, task_id: 3 }, points: 110, task: { id: 3, name: 'task #3', tags: [{ id: 3, name: 'tag #3', task_id: 3 }] } }] }]
  end

  it do
    expect(relation.to_a).to eq expected_data

    # It produces follow log:
    # I, [2018-01-02T14:54:46.729220 #29925]  INFO -- : (0.000391s) SELECT "users"."id", "users"."name" FROM "users" ORDER BY "users"."id"
    # D, [2018-01-02T14:54:46.730154 #29925] DEBUG -- : MONGODB | 0.0.0.0:27017 | rom_mongo.aggregate | STARTED | {"aggregate"=>"user_tasks", "pipeline"=>[{"$match"=>{"user_id"=>{"$in"=>[1, 2, 3]}}}, {"$group"=>{"_id"=>{"user_id"=>"$user_id", "task_id"=>"$task_id"}, "points"=>{"$sum"=>"$points"}}}, {"$sort"=>{"user_id"=>1, "task_id"=>1}}], "cursor"=>{}}
    # D, [2018-01-02T14:54:46.731589 #29925] DEBUG -- : MONGODB | 0.0.0.0:27017 | rom_mongo.aggregate | SUCCEEDED | 0.001343s
    # I, [2018-01-02T14:54:46.732655 #29925]  INFO -- : (0.000367s) SELECT "tasks"."id", "tasks"."name" FROM "tasks" WHERE ("id" IN (3, 2, 1)) ORDER BY "tasks"."id"
    # I, [2018-01-02T14:54:46.733825 #29925]  INFO -- : (0.000368s) SELECT "tags"."id", "tags"."name", "tags"."task_id" FROM "tags" INNER JOIN "tasks" ON ("tasks"."id" = "tags"."task_id") WHERE ("tags"."task_id" IN (1, 2, 3)) ORDER BY "tags"."id"
    # FIXME: Why there is a 'inner join' into tags relation?
  end
end
