"""
SQL models for the new Premium system
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Float, Table, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from flask_sqlalchemy import SQLAlchemy
from app import db

# Premium tier definitions with server limits and pricing
PREMIUM_TIERS = {
    0: {
        "name": "Free",
        "max_servers": 1,
        "price_gbp": 0,
        "features": ["killfeed"]
    },
    1: {
        "name": "Survivor",
        "max_servers": 2,
        "price_gbp": 5,
        "features": ["killfeed", "basic_stats", "leaderboards"]
    },
    2: {
        "name": "Mercenary",
        "max_servers": 5,
        "price_gbp": 10,
        "features": ["killfeed", "basic_stats", "leaderboards", "rivalries", "bounties", 
                    "player_links", "economy", "advanced_analytics"]
    },
    3: {
        "name": "Warlord",
        "max_servers": 10,
        "price_gbp": 20,
        "features": ["killfeed", "basic_stats", "leaderboards", "rivalries", "bounties", 
                    "player_links", "economy", "advanced_analytics", "factions", "events"]
    },
    4: {
        "name": "Overlord",
        "max_servers": 25,
        "price_gbp": 50,
        "features": ["killfeed", "basic_stats", "leaderboards", "rivalries", "bounties", 
                    "player_links", "economy", "advanced_analytics", "factions", "events",
                    "custom_embeds", "full_automation"]
    }
}

# Guild model with premium tier relationship
class DiscordGuild(db.Model):
    """Discord Guild model with premium tier information"""
    __tablename__ = 'discord_guilds'
    
    id = Column(Integer, primary_key=True)
    guild_id = Column(String(64), unique=True, nullable=False, index=True)
    name = Column(String(128), nullable=False)
    premium_tier = Column(Integer, default=0, nullable=False)
    premium_expires_at = Column(DateTime, nullable=True)
    
    # Theming options
    color_primary = Column(String(16), default="#7289DA")
    color_secondary = Column(String(16), default="#FFFFFF")
    color_accent = Column(String(16), default="#23272A")
    icon_url = Column(String(256), nullable=True)
    
    # Admin settings
    admin_role_id = Column(String(64), nullable=True)
    mod_role_id = Column(String(64), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    servers = relationship("GameServer", back_populates="guild", cascade="all, delete-orphan")
    subscriptions = relationship("PremiumSubscription", back_populates="guild", cascade="all, delete-orphan")
    
    def get_max_servers(self):
        """Get maximum servers allowed for this guild's premium tier"""
        tier_data = PREMIUM_TIERS.get(self.premium_tier, PREMIUM_TIERS[0])
        return tier_data.get("max_servers", 1)
    
    def has_feature_access(self, feature_name):
        """Check if guild is not None has access to a specific feature"""
        # Get tier data, defaulting to Free tier if invalid
        tier_data = PREMIUM_TIERS.get(self.premium_tier, PREMIUM_TIERS[0])
        
        # Check if premium has expired
        if self.premium_expires_at and self.premium_expires_at < datetime.utcnow():
            # Premium expired, use free tier features
            return feature_name in PREMIUM_TIERS[0]["features"]
            
        # Check if feature is available for this tier
        return feature_name in tier_data["features"]
    
    def check_premium_status(self):
        """Check if premium status is valid and update if expired"""
        # If no expiration date or tier is 0, no need to check
        if self.premium_tier == 0 or not self.premium_expires_at:
            return self.premium_tier
            
        # Check if premium has expired
        if self.premium_expires_at < datetime.utcnow():
            self.premium_tier = 0
            return 0
            
        return self.premium_tier
    
    def __repr__(self):
        return f"Guild(id={self.id}, name='{self.name}', premium_tier={self.premium_tier})"


class GameServer(db.Model):
    """Game Server model linked to a Discord Guild"""
    __tablename__ = 'game_servers'
    
    id = Column(Integer, primary_key=True)
    server_id = Column(String(64), unique=True, nullable=False, index=True)
    guild_id = Column(Integer, ForeignKey('discord_guilds.id'), nullable=False)
    server_name = Column(String(128), nullable=False)
    
    # Server identifiers - ensure original_server_id is ALWAYS a numeric ID
    original_server_id = Column(String(32), nullable=True)
    
    # SFTP connection details
    sftp_host = Column(String(128), nullable=True)
    sftp_port = Column(Integer, default=22)
    sftp_username = Column(String(64), nullable=True)
    sftp_password = Column(String(256), nullable=True)
    sftp_enabled = Column(Boolean, default=False)
    
    # File paths
    log_parser_path = Column(String(256), nullable=True)
    csv_parser_path = Column(String(256), nullable=True)
    
    # Processing state
    last_csv_line = Column(Integer, default=0)
    last_log_line = Column(Integer, default=0)
    historical_parse_done = Column(Boolean, default=False)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    guild = relationship("DiscordGuild", back_populates="servers")
    
    def __repr__(self):
        return f"<GameServer {self.server_name} ({self.server_id})>"


class PremiumSubscription(db.Model):
    """Premium subscription tracking"""
    __tablename__ = 'premium_subscriptions'
    
    id = Column(Integer, primary_key=True)
    guild_id = Column(Integer, ForeignKey('discord_guilds.id'), nullable=False)
    tier = Column(Integer, default=0, nullable=False)
    
    # Payment tracking
    payment_id = Column(String(128), nullable=True)
    payment_method = Column(String(64), nullable=True)
    amount_paid = Column(Float, nullable=True)
    currency = Column(String(8), default="GBP")
    
    # Subscription period
    starts_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=True)
    auto_renew = Column(Boolean, default=False)
    
    # Status
    status = Column(String(32), default="active")  # active, expired, cancelled
    
    # Metadata for admin tracking
    created_by_user_id = Column(String(64), nullable=True)
    notes = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    guild = relationship("DiscordGuild", back_populates="subscriptions")
    
    def __repr__(self):
        return f"<PremiumSubscription Tier {self.tier} for Guild {self.guild_id}>"