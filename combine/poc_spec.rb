#!/usr/bin/env ruby

require 'bundler/inline'

gemfile(true) do
  source 'https://rubygems.org'

  gem 'pry-byebug'

  gem 'dry-types'
  gem 'rom'
  gem 'rom-sql', '2.3.0'
  gem 'pg'
  gem 'rspec'
end

module Types
  include Dry::Types.module
end

require 'rspec/autorun'

RSpec.configure do |config|
  config.color = true
  config.formatter = :documentation
end

RSpec.describe 'Cross gateway associations' do
  let(:conn1) { Sequel.connect('postgres://localhost/db_1') }
  let(:conn2) { Sequel.connect('postgres://localhost/db_2') }
  let(:conf)  { ROM::Configuration.new(db_1: [:sql, conn1], db_2: [:sql, conn2]) }
  let(:container) { ROM.container(conf) }

  context 'when primary key is a composite' do
    before do
      conn1.logger = conn2.logger = Logger.new(STDERR)

      conn1.drop_table?(:projects)
      conn1.drop_table?(:users)
      conn2.drop_table?(:tasks)
      conn2.drop_table?(:tags)

      conn1.create_table :projects do
        primary_key :id
        column :name, String
      end

      conn1.create_table :users do
        primary_key :id
        column :name, String
        column :project_id, Integer
      end

      conn2.create_table :tasks do
        primary_key :id
        column :title, String
        column :user_id, Integer
      end

      conn2.create_table :tags do
        primary_key :id
        column :name, String
        column :task_id, Integer
      end

      conf.relation(:projects, gateway: :db_1) do
        schema(infer: true) do
          associations do
            has_many :users
          end
        end
      end

      conf.relation(:users, gateway: :db_1) do
        schema(infer: true) do
          associations do
            has_many :tasks, foreign_key: :user_id, override: true, view: :for_users
            belongs_to :project
          end
        end

        def for_tasks(_assoc, tasks)
          where(id: tasks.pluck(:user_id))
        end
      end

      conf.relation(:tasks, gateway: :db_2) do
        schema(infer: true) do
          associations do
            belongs_to :user, foreign_key: :user_id, override: true, view: :for_tasks
            has_many :tags
          end
        end

        def for_users(_assoc, users)
          where(user_id: users.pluck(:id))
        end
      end

      conf.relation(:tags, gateway: :db_2) do
        schema(infer: true) do
          associations do
            belongs_to :task
          end
        end
      end
    end

    let(:project) { container.relations.projects.command(:create).(name: 'rom-rb') }
    let(:jane) { container.relations.users.command(:create).(name: 'Jane', project_id: project[:id]) }
    let(:task) { container.relations.tasks.command(:create).(user_id: jane[:id], title: 'Jane task') }
    let!(:tag) { container.relations.tags.command(:create).(task_id: task[:id], name: 'green') }

    let(:left_to_right) do
      container.relations.tags.combine(task: [{ user: :project }])
    end

    let(:right_to_left) do
      container.relations.projects.combine(users: [{ tasks: :tags }]).to_a
    end

    it do
      # I, [2017-12-28T18:44:03.631672 #43418]  INFO -- : (0.000331s) SELECT "tags"."id", "tags"."name", "tags"."task_id" FROM "tags" ORDER BY "tags"."id"
      # I, [2017-12-28T18:44:03.632437 #43418]  INFO -- : (0.000217s) SELECT "tasks"."id", "tasks"."title", "tasks"."user_id" FROM "tasks" WHERE ("tasks"."id" IN (1)) ORDER BY "tasks"."id"
      # I, [2017-12-28T18:44:03.633091 #43418]  INFO -- : (0.000234s) SELECT "users"."id", "users"."name", "users"."project_id" FROM "users" WHERE ("users"."id" IN (1)) ORDER BY "users"."id"
      # I, [2017-12-28T18:44:03.633590 #43418]  INFO -- : (0.000195s) SELECT "projects"."id", "projects"."name" FROM "projects" WHERE ("projects"."id" IN (1)) ORDER BY "projects"."id"
      expect(left_to_right.to_a).to eq(
        [{ id: 1, name: 'green', task_id: 1, task: { id: 1, title: 'Jane task', user_id: 1, user: { id: 1, name: 'Jane', project_id: 1, project: { id: 1, name: 'rom-rb' } } } }]
      )
    end

    it do
      # I, [2017-12-28T18:44:40.132631 #43418]  INFO -- : (0.000371s) SELECT "projects"."id", "projects"."name" FROM "projects" ORDER BY "projects"."id"
      # I, [2017-12-28T18:44:40.133639 #43418]  INFO -- : (0.000279s) SELECT "users"."id", "users"."name", "users"."project_id" FROM "users" INNER JOIN "projects" ON ("projects"."id" = "users"."project_id") WHERE (
      # "users"."project_id" IN (1)) ORDER BY "users"."id"
      # I, [2017-12-28T18:44:40.134237 #43418]  INFO -- : (0.000230s) SELECT "tasks"."id", "tasks"."title", "tasks"."user_id" FROM "tasks" WHERE ("user_id" IN (1)) ORDER BY "tasks"."id"
      # I, [2017-12-28T18:44:40.134907 #43418]  INFO -- : (0.000246s) SELECT "tags"."id", "tags"."name", "tags"."task_id" FROM "tags" INNER JOIN "tasks" ON ("tasks"."id" = "tags"."task_id") WHERE ("tags"."task_id"
      # IN (1)) ORDER BY "tags"."id"
      expect(right_to_left).to eq(
        [{ id: 1, name: 'rom-rb', users: [{ id: 1, name: 'Jane', project_id: 1, tasks: [{ id: 1, title: 'Jane task', user_id: 1, tags: [{ id: 1, name: 'green', task_id: 1 }] }] }] }]
      )
    end
  end
end
