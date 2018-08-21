CREATE DATABASE  IF NOT EXISTS `benchmark_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */;
USE `benchmark_db`;
-- MySQL dump 10.13  Distrib 8.0.12, for macos10.13 (x86_64)
--
-- Host: localhost    Database: benchmark_db
-- ------------------------------------------------------
-- Server version	8.0.12

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
 SET NAMES utf8 ;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `hashtags`
--

DROP TABLE IF EXISTS `hashtags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `hashtags` (
  `id` bigint(16) NOT NULL,
  `hashtag` varchar(45) DEFAULT NULL,
  `indices` varchar(45) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `media`
--

DROP TABLE IF EXISTS `media`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `media` (
  `id` bigint(16) NOT NULL,
  `media` varchar(45) DEFAULT NULL,
  `display_url` varchar(45) DEFAULT NULL,
  `expanded_url` varchar(45) DEFAULT NULL,
  `m_id_str` varchar(45) DEFAULT NULL,
  `indices` longtext,
  `media_url` varchar(45) DEFAULT NULL,
  `media_url_https` varchar(45) DEFAULT NULL,
  `type` varchar(45) DEFAULT NULL,
  `sizes` longtext,
  `m_id` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `symbols`
--

DROP TABLE IF EXISTS `symbols`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `symbols` (
  `id` bigint(16) DEFAULT NULL,
  `symbol` varchar(45) DEFAULT NULL,
  `indices` varchar(45) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tweets`
--

DROP TABLE IF EXISTS `tweets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `tweets` (
  `created_at` varchar(100) DEFAULT NULL,
  `id` bigint(20) NOT NULL,
  `id_str` varchar(100) DEFAULT NULL,
  `text` varchar(200) DEFAULT NULL,
  `source` varchar(100) DEFAULT NULL,
  `truncated` varchar(10) DEFAULT NULL,
  `in_reply_to_status_id` bigint(10) DEFAULT NULL,
  `in_reply_to_status_id_str` varchar(100) DEFAULT NULL,
  `in_reply_to_user_id` bigint(10) DEFAULT NULL,
  `in_reply_to_user_id_str` varchar(100) DEFAULT NULL,
  `in_reply_to_screen_name` varchar(100) DEFAULT NULL,
  `user_id` bigint(10) DEFAULT NULL,
  `coordinates` varchar(10) DEFAULT NULL,
  `coordinates_type` varchar(45) DEFAULT NULL,
  `place_country` varchar(45) DEFAULT NULL,
  `place_country_code` varchar(100) DEFAULT NULL,
  `place_full_name` varchar(45) DEFAULT NULL,
  `place_id` varchar(45) DEFAULT NULL,
  `place_name` varchar(45) DEFAULT NULL,
  `place_type` varchar(45) DEFAULT NULL,
  `place_url` varchar(45) DEFAULT NULL,
  `quote_count` int(11) DEFAULT NULL,
  `reply_count` int(11) DEFAULT NULL,
  `favorite_count` int(11) DEFAULT NULL,
  `favorited` varchar(10) DEFAULT NULL,
  `retweeted` varchar(100) DEFAULT NULL,
  `filter_level` varchar(100) DEFAULT NULL,
  `lang` varchar(100) DEFAULT NULL,
  `quoted_status_id` varchar(45) DEFAULT NULL,
  `quoted_status_id_str` varchar(45) DEFAULT NULL,
  `quoted_status` longtext,
  `possibly_sensitive` varchar(45) DEFAULT NULL,
  `retweeted_status` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `universal`
--

DROP TABLE IF EXISTS `universal`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `universal` (
  `hashtags.hashtag` varchar(100) DEFAULT NULL,
  `hashtags.id` bigint(20) DEFAULT NULL,
  `hashtags.indices` varchar(10) DEFAULT NULL,
  `media.display_url` varchar(100) DEFAULT NULL,
  `media.expanded_url` varchar(100) DEFAULT NULL,
  `media.id` bigint(20) DEFAULT NULL,
  `media.indices` varchar(10) DEFAULT NULL,
  `media.m_id` bigint(20) DEFAULT NULL,
  `media.m_id_str` varchar(100) DEFAULT NULL,
  `media.media` varchar(100) DEFAULT NULL,
  `media.media_url` varchar(100) DEFAULT NULL,
  `media.media_url_https` varchar(100) DEFAULT NULL,
  `media.sizes` varchar(10) DEFAULT NULL,
  `media.type` varchar(10) DEFAULT NULL,
  `symbols.id` varchar(10) DEFAULT NULL,
  `symbols.indices` varchar(10) DEFAULT NULL,
  `symbols.symbol` varchar(10) DEFAULT NULL,
  `tweets.coordinates` varchar(100) DEFAULT NULL,
  `tweets.coordinates_type` varchar(10) DEFAULT NULL,
  `tweets.created_at` varchar(100) DEFAULT NULL,
  `tweets.favorite_count` int(20) DEFAULT NULL,
  `tweets.favorited` varchar(10) DEFAULT NULL,
  `tweets.filter_level` varchar(10) DEFAULT NULL,
  `tweets.id` bigint(20) NOT NULL,
  `tweets.id_str` varchar(100) DEFAULT NULL,
  `tweets.in_reply_to_screen_name` varchar(100) DEFAULT NULL,
  `tweets.in_reply_to_status_id` varchar(100) DEFAULT NULL,
  `tweets.in_reply_to_status_id_str` varchar(100) DEFAULT NULL,
  `tweets.in_reply_to_user_id` varchar(100) DEFAULT NULL,
  `tweets.in_reply_to_user_id_str` varchar(100) DEFAULT NULL,
  `tweets.lang` varchar(100) DEFAULT NULL,
  `tweets.place_country` varchar(100) DEFAULT NULL,
  `tweets.place_country_code` varchar(100) DEFAULT NULL,
  `tweets.place_full_name` varchar(100) DEFAULT NULL,
  `tweets.place_id` varchar(100) DEFAULT NULL,
  `tweets.place_name` varchar(100) DEFAULT NULL,
  `tweets.place_type` varchar(100) DEFAULT NULL,
  `tweets.place_url` varchar(100) DEFAULT NULL,
  `tweets.possibly_sensitive` varchar(100) DEFAULT NULL,
  `tweets.quote_count` int(20) DEFAULT NULL,
  `tweets.quoted_status` varchar(200) DEFAULT NULL,
  `tweets.quoted_status_id` varchar(100) DEFAULT NULL,
  `tweets.quoted_status_id_str` varchar(100) DEFAULT NULL,
  `tweets.reply_count` int(20) DEFAULT NULL,
  `tweets.retweeted` varchar(100) DEFAULT NULL,
  `tweets.retweeted_status` varchar(100) DEFAULT NULL,
  `tweets.source` varchar(100) DEFAULT NULL,
  `tweets.text` varchar(200) DEFAULT NULL,
  `tweets.truncated` varchar(100) DEFAULT NULL,
  `tweets.user_id` bigint(20) DEFAULT NULL,
  `urls.display_url` varchar(100) DEFAULT NULL,
  `urls.expanded_url` longtext,
  `urls.id` bigint(20) DEFAULT NULL,
  `urls.indices` varchar(100) DEFAULT NULL,
  `urls.url` varchar(100) DEFAULT NULL,
  `user_mentions.id` bigint(20) DEFAULT NULL,
  `user_mentions.indices` varchar(100) DEFAULT NULL,
  `user_mentions.name` varchar(100) DEFAULT NULL,
  `user_mentions.screen_name` varchar(100) DEFAULT NULL,
  `user_mentions.u_id` bigint(20) DEFAULT NULL,
  `user_mentions.u_id_str` varchar(100) DEFAULT NULL,
  `users.contributors_enabled` varchar(100) DEFAULT NULL,
  `users.created_at` varchar(100) DEFAULT NULL,
  `users.default_profile` varchar(100) DEFAULT NULL,
  `users.default_profile_image` varchar(100) DEFAULT NULL,
  `users.description` longtext,
  `users.favourites_count` int(20) DEFAULT NULL,
  `users.follow_request_sent` varchar(100) DEFAULT NULL,
  `users.followers_count` int(20) DEFAULT NULL,
  `users.following` varchar(100) DEFAULT NULL,
  `users.friends_count` int(20) DEFAULT NULL,
  `users.geo-enabled` varchar(100) DEFAULT NULL,
  `users.id` bigint(20) DEFAULT NULL,
  `users.id_str` varchar(100) DEFAULT NULL,
  `users.is_translator` varchar(100) DEFAULT NULL,
  `users.lang` varchar(100) DEFAULT NULL,
  `users.listed_count` int(20) DEFAULT NULL,
  `users.location` varchar(100) DEFAULT NULL,
  `users.name` varchar(100) DEFAULT NULL,
  `users.notifications` varchar(100) DEFAULT NULL,
  `users.profile_background_color` varchar(100) DEFAULT NULL,
  `users.profile_background_image_url` varchar(200) DEFAULT NULL,
  `users.profile_background_image_url_https` varchar(200) DEFAULT NULL,
  `users.profile_background_tile` varchar(100) DEFAULT NULL,
  `users.profile_image_url` varchar(200) DEFAULT NULL,
  `users.profile_image_url_https` varchar(200) DEFAULT NULL,
  `users.profile_link_color` varchar(100) DEFAULT NULL,
  `users.profile_sidebar_border_color` varchar(100) DEFAULT NULL,
  `users.profile_sidebar_fill_color` varchar(100) DEFAULT NULL,
  `users.profile_text_color` varchar(100) DEFAULT NULL,
  `users.profile_use_background_image` varchar(100) DEFAULT NULL,
  `users.protected` varchar(100) DEFAULT NULL,
  `users.screen_name` varchar(100) DEFAULT NULL,
  `users.statuses_count` int(20) DEFAULT NULL,
  `users.time_zone` varchar(100) DEFAULT NULL,
  `users.translator_type` varchar(100) DEFAULT NULL,
  `users.url` varchar(200) DEFAULT NULL,
  `users.utc_offset` varchar(100) DEFAULT NULL,
  `users.verified` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`tweets.id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `universal_indexed`
--

DROP TABLE IF EXISTS `universal_indexed`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `universal_indexed` (
  `hashtags.hashtag` varchar(100) DEFAULT NULL,
  `hashtags.id` bigint(20) DEFAULT NULL,
  `hashtags.indices` varchar(10) DEFAULT NULL,
  `media.display_url` varchar(100) DEFAULT NULL,
  `media.expanded_url` varchar(100) DEFAULT NULL,
  `media.id` bigint(20) DEFAULT NULL,
  `media.indices` varchar(10) DEFAULT NULL,
  `media.m_id` bigint(20) DEFAULT NULL,
  `media.m_id_str` varchar(100) DEFAULT NULL,
  `media.media` varchar(100) DEFAULT NULL,
  `media.media_url` varchar(100) DEFAULT NULL,
  `media.media_url_https` varchar(100) DEFAULT NULL,
  `media.sizes` varchar(10) DEFAULT NULL,
  `media.type` varchar(10) DEFAULT NULL,
  `symbols.id` varchar(10) DEFAULT NULL,
  `symbols.indices` varchar(10) DEFAULT NULL,
  `symbols.symbol` varchar(10) DEFAULT NULL,
  `tweets.coordinates` varchar(100) DEFAULT NULL,
  `tweets.coordinates_type` varchar(10) DEFAULT NULL,
  `tweets.created_at` varchar(100) DEFAULT NULL,
  `tweets.favorite_count` int(20) DEFAULT NULL,
  `tweets.favorited` varchar(10) DEFAULT NULL,
  `tweets.filter_level` varchar(10) DEFAULT NULL,
  ` tweets.id` bigint(20) NOT NULL,
  `tweets.id_str` varchar(100) DEFAULT NULL,
  `tweets.in_reply_to_screen_name` varchar(100) DEFAULT NULL,
  `tweets.in_reply_to_status_id` varchar(100) DEFAULT NULL,
  `tweets.in_reply_to_status_id_str` varchar(100) DEFAULT NULL,
  `tweets.in_reply_to_user_id` varchar(100) DEFAULT NULL,
  `tweets.in_reply_to_user_id_str` varchar(100) DEFAULT NULL,
  `tweets.lang` varchar(100) DEFAULT NULL,
  `tweets.place_country` varchar(100) DEFAULT NULL,
  `tweets.place_country_code` varchar(100) DEFAULT NULL,
  `tweets.place_full_name` varchar(100) DEFAULT NULL,
  `tweets.place_id` varchar(100) DEFAULT NULL,
  `tweets.place_name` varchar(100) DEFAULT NULL,
  `tweets.place_type` varchar(100) DEFAULT NULL,
  `tweets.place_url` varchar(100) DEFAULT NULL,
  `tweets.possibly_sensitive` varchar(100) DEFAULT NULL,
  `tweets.quote_count` int(20) DEFAULT NULL,
  `tweets.quoted_status` varchar(200) DEFAULT NULL,
  `tweets.quoted_status_id` varchar(100) DEFAULT NULL,
  `tweets.quoted_status_id_str` varchar(100) DEFAULT NULL,
  `tweets.reply_count` int(20) DEFAULT NULL,
  `tweets.retweeted` varchar(100) DEFAULT NULL,
  `tweets.retweeted_status` varchar(100) DEFAULT NULL,
  `tweets.source` varchar(100) DEFAULT NULL,
  `tweets.text` varchar(200) DEFAULT NULL,
  `tweets.truncated` varchar(100) DEFAULT NULL,
  `tweets.user_id` bigint(20) DEFAULT NULL,
  `urls.display_url` varchar(100) DEFAULT NULL,
  `urls.expanded_url` longtext,
  `urls.id` bigint(20) DEFAULT NULL,
  `urls.indices` varchar(100) DEFAULT NULL,
  `urls.url` varchar(100) DEFAULT NULL,
  `user_mentions.id` bigint(20) DEFAULT NULL,
  `user_mentions.indices` varchar(100) DEFAULT NULL,
  `user_mentions.name` varchar(100) DEFAULT NULL,
  `user_mentions.screen_name` varchar(100) DEFAULT NULL,
  `user_mentions.u_id` bigint(20) DEFAULT NULL,
  `user_mentions.u_id_str` varchar(100) DEFAULT NULL,
  `users.contributors_enabled` varchar(100) DEFAULT NULL,
  `users.created_at` varchar(100) DEFAULT NULL,
  `users.default_profile` varchar(100) DEFAULT NULL,
  `users.default_profile_image` varchar(100) DEFAULT NULL,
  `users.description` longtext,
  `users.favourites_count` int(20) DEFAULT NULL,
  `users.follow_request_sent` varchar(100) DEFAULT NULL,
  `users.followers_count` int(20) DEFAULT NULL,
  `users.following` varchar(100) DEFAULT NULL,
  `users.friends_count` int(20) DEFAULT NULL,
  `users.geo-enabled` varchar(100) DEFAULT NULL,
  `users.id` bigint(20) DEFAULT NULL,
  `users.id_str` varchar(100) DEFAULT NULL,
  `users.is_translator` varchar(100) DEFAULT NULL,
  `users.lang` varchar(100) DEFAULT NULL,
  `users.listed_count` int(20) DEFAULT NULL,
  `users.location` varchar(100) DEFAULT NULL,
  `users.name` varchar(100) DEFAULT NULL,
  `users.notifications` varchar(100) DEFAULT NULL,
  `users.profile_background_color` varchar(100) DEFAULT NULL,
  `users.profile_background_image_url` varchar(200) DEFAULT NULL,
  `users.profile_background_image_url_https` varchar(200) DEFAULT NULL,
  `users.profile_background_tile` varchar(100) DEFAULT NULL,
  `users.profile_image_url` varchar(200) DEFAULT NULL,
  `users.profile_image_url_https` varchar(200) DEFAULT NULL,
  `users.profile_link_color` varchar(100) DEFAULT NULL,
  `users.profile_sidebar_border_color` varchar(100) DEFAULT NULL,
  `users.profile_sidebar_fill_color` varchar(100) DEFAULT NULL,
  `users.profile_text_color` varchar(100) DEFAULT NULL,
  `users.profile_use_background_image` varchar(100) DEFAULT NULL,
  `users.protected` varchar(100) DEFAULT NULL,
  `users.screen_name` varchar(100) DEFAULT NULL,
  `users.statuses_count` int(20) DEFAULT NULL,
  `users.time_zone` varchar(100) DEFAULT NULL,
  `users.translator_type` varchar(100) DEFAULT NULL,
  `users.url` varchar(200) DEFAULT NULL,
  `users.utc_offset` varchar(100) DEFAULT NULL,
  `users.verified` varchar(100) DEFAULT NULL,
  PRIMARY KEY (` tweets.id`),
  KEY `user.id_index` (`users.id`) USING BTREE,
  KEY `tweet.id_index` (` tweets.id`) USING BTREE,
  KEY `followers_count_index` (`users.followers_count`) USING BTREE,
  KEY `friends_count_index` (`users.friends_count`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `urls`
--

DROP TABLE IF EXISTS `urls`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `urls` (
  `id` bigint(16) DEFAULT NULL,
  `url` varchar(45) DEFAULT NULL,
  `display_url` longtext,
  `expanded_url` longtext,
  `indices` varchar(45) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_mentions`
--

DROP TABLE IF EXISTS `user_mentions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `user_mentions` (
  `id` bigint(16) NOT NULL,
  `name` longtext,
  `indices` varchar(45) DEFAULT NULL,
  `screen_name` longtext,
  `u_id` bigint(16) DEFAULT NULL,
  `u_id_str` longtext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `users` (
  `id` varchar(45) NOT NULL,
  `id_str` varchar(45) DEFAULT NULL,
  `name` longtext,
  `screen_name` varchar(45) DEFAULT NULL,
  `location` longtext,
  `url` longtext,
  `description` longtext,
  `translator_type` varchar(45) DEFAULT NULL,
  `protected` varchar(45) DEFAULT NULL,
  `verified` varchar(45) DEFAULT NULL,
  `followers_count` longtext,
  `friends_count` varchar(45) DEFAULT NULL,
  `listed_count` varchar(45) DEFAULT NULL,
  `favourites_count` longtext,
  `statuses_count` varchar(45) DEFAULT NULL,
  `created_at` varchar(45) DEFAULT NULL,
  `utc_offset` varchar(45) DEFAULT NULL,
  `time_zone` varchar(45) DEFAULT NULL,
  `geo-enabled` varchar(45) DEFAULT NULL,
  `lang` varchar(45) DEFAULT NULL,
  `contributors_enabled` varchar(45) DEFAULT NULL,
  `is_translator` varchar(45) DEFAULT NULL,
  `profile_background_color` longtext,
  `profile_background_image_url` longtext,
  `profile_background_tile` longtext,
  `profile_image_url` longtext,
  `profile_image_url_https` longtext,
  `profile_link_color` longtext,
  `profile_sidebar_border_color` varchar(45) DEFAULT NULL,
  `profile_sidebar_fill_color` varchar(45) DEFAULT NULL,
  `profile_text_color` varchar(45) DEFAULT NULL,
  `profile_use_background_image` varchar(45) DEFAULT NULL,
  `default_profile` varchar(45) DEFAULT NULL,
  `default_profile_image` varchar(45) DEFAULT NULL,
  `following` varchar(45) DEFAULT NULL,
  `follow_request_sent` varchar(45) DEFAULT NULL,
  `notifications` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-08-21 17:20:04
