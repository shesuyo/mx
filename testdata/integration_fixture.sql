DROP TABLE IF EXISTS `history`, `gem`, `weapon`, `user`;

CREATE TABLE `user` (
	`id` int unsigned NOT NULL AUTO_INCREMENT,
	`name` varchar(64) NOT NULL,
	`age` int NOT NULL DEFAULT 0,
	`uid` int NOT NULL DEFAULT 0,
	`ctime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`utime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`is_deleted` tinyint(1) NOT NULL DEFAULT 0,
	PRIMARY KEY (`id`),
	KEY `idx_user_uid` (`uid`),
	KEY `idx_user_age` (`age`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `weapon` (
	`id` int unsigned NOT NULL AUTO_INCREMENT,
	`user_id` int unsigned NOT NULL,
	`name` varchar(64) NOT NULL,
	`lv` varchar(32) NOT NULL,
	`ctime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`utime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`),
	KEY `idx_weapon_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `gem` (
	`id` int unsigned NOT NULL AUTO_INCREMENT,
	`user_id` int unsigned NOT NULL,
	`name` varchar(64) NOT NULL,
	`lv` varchar(32) NOT NULL,
	`ctime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`utime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`),
	KEY `idx_gem_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `history` (
	`id` int unsigned NOT NULL AUTO_INCREMENT,
	`gem_id` int unsigned NOT NULL,
	`remark` varchar(128) NOT NULL,
	PRIMARY KEY (`id`),
	KEY `idx_history_gem_id` (`gem_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `user` (`id`, `name`, `age`, `uid`, `ctime`, `utime`, `is_deleted`) VALUES
	(1, 'john_alpha', 25, 1001, '2026-01-01 10:00:00', '2026-01-01 10:00:00', 0),
	(2, 'test_betty', 32, 1002, '2026-01-02 10:00:00', '2026-01-02 10:00:00', 0),
	(3, 'alice_john', 28, 1003, '2026-01-03 10:00:00', '2026-01-03 10:00:00', 0),
	(4, 'test_john', 25, 1004, '2026-01-04 10:00:00', '2026-01-04 10:00:00', 0),
	(5, 'maria', 41, 1005, '2026-01-05 10:00:00', '2026-01-05 10:00:00', 0);

INSERT INTO `weapon` (`id`, `user_id`, `name`, `lv`, `ctime`, `utime`) VALUES
	(1, 1, 'wood_sword', '1', '2026-01-01 10:10:00', '2026-01-01 10:10:00'),
	(2, 2, 'iron_sword', '2', '2026-01-02 10:10:00', '2026-01-02 10:10:00'),
	(3, 3, 'silver_spear', '3', '2026-01-03 10:10:00', '2026-01-03 10:10:00'),
	(4, 4, 'gold_bow', '4', '2026-01-04 10:10:00', '2026-01-04 10:10:00'),
	(5, 5, 'moon_staff', '5', '2026-01-05 10:10:00', '2026-01-05 10:10:00');

INSERT INTO `gem` (`id`, `user_id`, `name`, `lv`, `ctime`, `utime`) VALUES
	(1, 1, 'ruby', '1', '2026-01-01 10:20:00', '2026-01-01 10:20:00'),
	(2, 1, 'sapphire', '2', '2026-01-01 10:21:00', '2026-01-01 10:21:00'),
	(3, 2, 'emerald', '2', '2026-01-02 10:20:00', '2026-01-02 10:20:00'),
	(4, 3, 'topaz', '3', '2026-01-03 10:20:00', '2026-01-03 10:20:00'),
	(5, 4, 'opal', '4', '2026-01-04 10:20:00', '2026-01-04 10:20:00');

INSERT INTO `history` (`id`, `gem_id`, `remark`) VALUES
	(1, 1, 'ruby-created'),
	(2, 1, 'ruby-upgraded'),
	(3, 2, 'sapphire-created'),
	(4, 3, 'topaz-created'),
	(5, 4, 'opal-created');

ALTER TABLE `user` AUTO_INCREMENT = 1000;
ALTER TABLE `weapon` AUTO_INCREMENT = 1000;
ALTER TABLE `gem` AUTO_INCREMENT = 1000;
ALTER TABLE `history` AUTO_INCREMENT = 1000;
