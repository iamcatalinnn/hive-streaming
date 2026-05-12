"""Pydantic models defining the data contracts for Silver and Gold layers."""

from enum import Enum
from typing import Optional
from pydantic import BaseModel, field_validator


# ── Enums ─────────────────────────────────────────────────────────────────────

class VideoQuality(str, Enum):
    Q144  = "144p"
    Q270  = "270p"
    Q360  = "360p"
    Q480  = "480p"
    Q540  = "540p"
    Q720  = "720p"
    Q1080 = "1080p"
    Q1440 = "1440p"
    Q2160 = "2160p"


class QoSLabel(str, Enum):
    GREEN  = "green"
    YELLOW = "yellow"
    RED    = "red"


# ── Config ────────────────────────────────────────────────────────────────────

class QoSConfig(BaseModel):
    buffering_green_threshold: float = 0.05
    buffering_red_threshold:   float = 0.35
    buffering_weight:          float = 0.5
    quality_weight:            float = 0.3
    stability_weight:          float = 0.2


# ── Base ──────────────────────────────────────────────────────────────────────

class TrafficBase(BaseModel):
    """
    Reusable base for any model that carries
    source + p2p traffic fields.
    Inherited by SilverSession and SilverQuality.
    """
    source_requests:        int
    source_responses:       float
    source_requested_bytes: int
    source_received_bytes:  int
    p2p_requests:           int
    p2p_responses:          float
    p2p_requested_bytes:    int
    p2p_received_bytes:     int

    @field_validator(
        'source_requests',
        'source_requested_bytes',
        'source_received_bytes',
        'p2p_requests',
        'p2p_requested_bytes',
        'p2p_received_bytes'
    )
    @classmethod
    def traffic_non_negative(cls, v):
        if v < 0:
            raise ValueError('traffic fields must be >= 0')
        return v

    @field_validator('source_responses', 'p2p_responses')
    @classmethod
    def responses_non_negative(cls, v):
        if v < 0:
            raise ValueError('responses must be >= 0')
        return v


# ── Silver ────────────────────────────────────────────────────────────────────

class SilverSession(TrafficBase):
    """
    One row per viewer per 30s heartbeat window.
    PK: customer_id + content_id + client_id + window_start
    """
    customer_id:       str
    content_id:        str
    client_id:         str
    window_start:      Optional[int] = None
    window_end:        int
    bufferings:        int
    buffering_time_ms: int
    has_quality_data:  bool

    @field_validator('bufferings', 'buffering_time_ms')
    @classmethod
    def must_be_non_negative(cls, v):
        if v < 0:
            raise ValueError('must be >= 0')
        return v


class SilverQuality(TrafficBase):
    """
    One row per viewer per window per quality level.
    PK: customer_id + content_id + client_id + window_start + quality
    """
    customer_id:  str
    content_id:   str
    client_id:    str
    window_end:   int
    quality:      VideoQuality


# ── Gold ──────────────────────────────────────────────────────────────────────

class Gold(BaseModel):
    """
    One row per viewer session (full aggregation).
    PK: customer_id + content_id + client_id
    """
    # --- identifiers ---
    customer_id:                  str
    content_id:                   str
    client_id:                    str

    # --- session timing ---
    session_start:                int
    session_end:                  int
    session_duration_min:         float

    # --- buffering ---
    total_bufferings:             int
    total_buffering_time_ms:      int
    buffering_ratio:              float

    # --- traffic ---
    total_source_requested_bytes: int
    total_source_received_bytes:  int
    total_p2p_requested_bytes:    int
    total_p2p_received_bytes:     int
    delivery_rate:                float
    p2p_ratio:                    float

    # --- quality ---
    dominant_quality:             Optional[VideoQuality]
    quality_switches:             int

    # --- qos ---
    qos_score:                    float
    qos_label:                    QoSLabel

    @field_validator(
        'total_bufferings',
        'total_buffering_time_ms',
        'total_source_requested_bytes',
        'total_source_received_bytes',
        'total_p2p_requested_bytes',
        'total_p2p_received_bytes',
        'quality_switches'
    )
    @classmethod
    def must_be_non_negative(cls, v):
        if v < 0:
            raise ValueError('must be >= 0')
        return v

    @field_validator('buffering_ratio', 'p2p_ratio', 'delivery_rate')
    @classmethod
    def ratio_between_0_and_1(cls, v):
        if not 0 <= v <= 1:
            raise ValueError('ratio must be between 0 and 1')
        return v

    @field_validator('qos_score')
    @classmethod
    def qos_score_valid(cls, v):
        if not 0 <= v <= 1:
            raise ValueError('qos_score must be between 0 and 1')
        return v
