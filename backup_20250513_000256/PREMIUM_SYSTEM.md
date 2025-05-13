# Premium System Documentation

The premium system provides a way to gate features behind premium tiers. It's designed to be simple to use while also being robust and reliable.

## Table of Contents

1. [Overview](#overview)
2. [Premium Tiers](#premium-tiers)
3. [Features](#features)
4. [Usage](#usage)
5. [Command Examples](#command-examples)
6. [API Examples](#api-examples)
7. [Database Schema](#database-schema)
8. [Migration Guide](#migration-guide)

## Overview

The premium system allows guilds to access different features based on their premium tier. Features are gated by tier, with higher tiers having access to more features.

Key principles of the premium system:
- Premium is **guild-based**, not user-based
- Feature access is determined by tier level
- Each tier has a maximum number of servers
- Tiers have automatic expiration dates

## Premium Tiers

The system has 5 premium tiers (0-4):

| Tier | Name      | Max Servers | Price (£/month) |
|------|-----------|-------------|-----------------|
| 0    | Free      | 1           | £0              |
| 1    | Survivor  | 2           | £5              |
| 2    | Mercenary | 5           | £10             |
| 3    | Warlord   | 10          | £20             |
| 4    | Overlord  | 25          | £50             |

## Features

Each premium tier has access to specific features:

### Tier 0 (Free)
- killfeed

### Tier 1 (Survivor)
- All Tier 0 features
- basic_stats
- leaderboards

### Tier 2 (Mercenary)
- All Tier 1 features
- rivalries
- bounties
- player_links
- economy
- advanced_analytics

### Tier 3 (Warlord)
- All Tier 2 features
- factions
- events

### Tier 4 (Overlord)
- All Tier 3 features
- custom_embeds
- full_automation

## Usage

### Checking Feature Access

To check if a guild has access to a feature, use the `PremiumFeature` class:

```python
from premium_feature_access import PremiumFeature

# Check access
has_access = await PremiumFeature.check_access(bot.db, guild_id, "basic_stats")

# Get guild's tier
tier = await PremiumFeature.get_guild_tier(bot.db, guild_id)

# Get all features and their access status
feature_status = await PremiumFeature.get_guild_feature_list(bot.db, guild_id)
```

### Requiring Premium Features for Commands

To require a premium feature for a command, use the `PremiumFeature.require` decorator:

```python
from premium_feature_access import PremiumFeature

@commands.command()
@PremiumFeature.require("basic_stats")
async def stats(ctx):
    # This command will only run if the guild has access to the basic_stats feature
    await ctx.send("Stats command")
```

### Requiring Premium Tier for Commands

To require a specific premium tier for a command, use the `PremiumFeature.require_tier` decorator:

```python
from premium_feature_access import PremiumFeature

@commands.command()
@PremiumFeature.require_tier(2)  # Requires Mercenary tier or higher
async def mercenary_command(ctx):
    # This command will only run if the guild has premium tier 2 or higher
    await ctx.send("Mercenary command")
```

### Access to Premium Guild Model

To get access to the `PremiumGuild` model, use the `get_by_guild_id` or `get_or_create` methods:

```python
from premium_mongodb_models import PremiumGuild

# Get a guild by ID
guild = await PremiumGuild.get_by_guild_id(bot.db, guild_id)

# Get or create a guild
guild = await PremiumGuild.get_or_create(bot.db, guild_id, guild_name)

# Check feature access
has_access = guild.has_feature_access("basic_stats")
```

## Command Examples

### Traditional Commands

```python
@commands.command()
@PremiumFeature.require("basic_stats")
async def stats(ctx):
    await ctx.send("Stats command")
```

### Slash Commands

```python
@app_commands.command(name="stats", description="View stats")
@app_commands.guild_only()
@PremiumFeature.require("basic_stats")
async def stats(interaction: discord.Interaction):
    await interaction.response.send_message("Stats command")
```

### Hybrid Commands

```python
@commands.hybrid_command(name="stats", description="View stats")
@PremiumFeature.require("basic_stats")
async def stats(ctx):
    await ctx.send("Stats command")
```

## API Examples

### Setting Premium Tier

```python
from premium_mongodb_models import PremiumGuild
from datetime import datetime, timedelta

# Get guild
guild = await PremiumGuild.get_by_guild_id(bot.db, guild_id)

# Set premium tier with 30-day expiration
expires_at = datetime.utcnow() + timedelta(days=30)
await guild.set_premium_tier(2, expires_at=expires_at, reason="Purchased via website")
```

### Checking Premium Status

```python
from premium_mongodb_models import PremiumGuild

# Get guild
guild = await PremiumGuild.get_by_guild_id(bot.db, guild_id)

# Check current tier (handles expiration check)
current_tier = guild.check_premium_status()

# Get maximum servers allowed
max_servers = guild.get_max_servers()
```

## Database Schema

The premium system uses three MongoDB collections:

### premium_guilds

```
{
    "_id": ObjectId,
    "guild_id": String,
    "name": String,
    "premium_tier": Int,
    "premium_expires_at": Date,
    "color_primary": String,
    "color_secondary": String,
    "color_accent": String,
    "icon_url": String,
    "admin_role_id": String,
    "mod_role_id": String,
    "servers": Array,
    "subscriptions": Array,
    "created_at": Date,
    "updated_at": Date
}
```

### premium_servers

```
{
    "_id": ObjectId,
    "server_id": String,
    "guild_id": String,
    "server_name": String,
    "original_server_id": String,
    "sftp_host": String,
    "sftp_port": Int,
    "sftp_username": String,
    "sftp_password": String,
    "sftp_enabled": Boolean,
    "log_parser_path": String,
    "csv_parser_path": String,
    "last_csv_line": Int,
    "last_log_line": Int,
    "historical_parse_done": Boolean,
    "created_at": Date,
    "updated_at": Date
}
```

### premium_payments

```
{
    "_id": ObjectId,
    "guild_id": String,
    "payment_id": String,
    "amount": Float,
    "currency": String,
    "status": String,
    "tier": Int,
    "duration_days": Int,
    "created_at": Date,
    "paid_at": Date,
    "expires_at": Date
}
```

## Migration Guide

To migrate from the old premium system to the new one, use the `premium_migration.py` script:

```bash
python premium_migration.py
```

This will:
1. Create the new premium collections if they don't exist
2. Migrate all guilds from the old system to the new one
3. Migrate all servers from the old system to the new one
4. Verify the migration was successful

During the migration period, you can use the `premium_compatibility.py` module to ensure backward compatibility with existing code.