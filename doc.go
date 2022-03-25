// Package mx is a mysql orm lib

// It is help us to use mysql

// 目前设计只支持单数据库，后续重构后支持多数据库。

// FEATURE:
// Create
// Delete
// Update
// Read
// SQL查询返回map[string]string
// SQL查询返回map[string]any
// SQL将查询的结构映射到结构体中
// SQL将查询的结构映射到结构体数组中
// BeforeCreate
// AfterCreate
// BeforeUpdate
// AfterUpdate
// AfterFind
// BeforeDelete
// AfterDelete
// PLAN:
// 支持多数据库
// 支持分表分库

// 提倡复杂的SQL直接写SQL语句，不使用ORM

package mx
