package main

import (
	"context"
	"database/sql"
	"fmt"

	userrepo "aieas_backend/internal/modules/user/repository"
)

type devUser struct {
	ID           uint64
	Account      string
	Phone        string
	Nickname     string
	PasswordHash string
	Role         string
	Status       string
}

var devUsers = []devUser{
	{ID: 1001, Account: "buyer001", Phone: "13800000003", Nickname: "竞拍用户001", PasswordHash: userrepo.HashPassword("Passw0rd!"), Role: "buyer", Status: "ACTIVE"},
	{ID: 2001, Account: "merchant001", Phone: "13800000002", Nickname: "商家001", PasswordHash: userrepo.HashPassword("Passw0rd!"), Role: "merchant", Status: "ACTIVE"},
	{ID: 9001, Account: "admin001", Phone: "13800000001", Nickname: "管理员001", PasswordHash: userrepo.HashPassword("AdminPassw0rd!"), Role: "admin", Status: "ACTIVE"},
	{ID: 1002, Account: "disabled001", Phone: "13800000004", Nickname: "停用用户001", PasswordHash: userrepo.HashPassword("Passw0rd!"), Role: "buyer", Status: "DISABLED"},
}

const upsertDevUserSQL = "\n" +
	"INSERT INTO `user` (`id`, `account`, `phone`, `nickname`, `password_hash`, `role`, `status`)\n" +
	"VALUES (?, ?, ?, ?, ?, ?, ?)\n" +
	"ON DUPLICATE KEY UPDATE\n" +
	"  `id` = VALUES(`id`),\n" +
	"  `account` = VALUES(`account`),\n" +
	"  `phone` = VALUES(`phone`),\n" +
	"  `nickname` = VALUES(`nickname`),\n" +
	"  `password_hash` = VALUES(`password_hash`),\n" +
	"  `role` = VALUES(`role`),\n" +
	"  `status` = VALUES(`status`)"

func seedDevUsers(ctx context.Context, db *sql.DB) error {
	for _, user := range devUsers {
		_, err := db.ExecContext(
			ctx,
			upsertDevUserSQL,
			user.ID,
			user.Account,
			user.Phone,
			user.Nickname,
			user.PasswordHash,
			user.Role,
			user.Status,
		)
		if err != nil {
			return fmt.Errorf("upsert dev user %s: %w", user.Account, err)
		}
	}
	return nil
}
