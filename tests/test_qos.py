"""
Tests for QoS scoring logic.
These test the scoring functions directly — no DataFrame needed.
"""

import pytest
from src.pipeline.models.schemas import QoSConfig
from src.pipeline.quality.qos import (
    _buffering_score,
    _quality_score,
    _stability_score,
    _qos_label
)


@pytest.fixture
def config():
    return QoSConfig()


def test_buffering_score_green(config):
    """Below 5% buffering ratio → score 1.0"""
    assert _buffering_score(0.01, config) == 1.0
    assert _buffering_score(0.04, config) == 1.0


def test_buffering_score_yellow(config):
    """Between 5% and 35% → score 0.75"""
    assert _buffering_score(0.10, config) == 0.75
    assert _buffering_score(0.34, config) == 0.75


def test_buffering_score_red(config):
    """Above 35% → score 0.25"""
    assert _buffering_score(0.40, config) == 0.25
    assert _buffering_score(0.90, config) == 0.25


def test_buffering_score_boundary_green(config):
    """Exactly at green threshold (5%) → yellow, not green"""
    assert _buffering_score(0.05, config) == 0.75


def test_buffering_score_boundary_red(config):
    """Exactly at red threshold (35%) → yellow, not red"""
    assert _buffering_score(0.35, config) == 0.75


def test_quality_score_highest(config):
    """2160p is highest quality → score 1.0"""
    assert _quality_score('2160p') == 1.0


def test_quality_score_lowest(config):
    """144p is lowest quality → score 1/9"""
    assert round(_quality_score('144p'), 4) == round(1/9, 4)


def test_quality_score_none(config):
    """No quality data → score 0.0"""
    assert _quality_score(None) == 0.0


def test_stability_score_stable(config):
    """Less than 1 switch/min → score 1.0"""
    assert _stability_score(0, 10.0) == 1.0
    assert _stability_score(5, 10.0) == 1.0  # 0.5/min


def test_stability_score_unstable(config):
    """More than 4 switches/min → score 0.25"""
    assert _stability_score(50, 10.0) == 0.25  # 5/min


def test_qos_label_green():
    """Score >= 0.75 → green"""
    assert _qos_label(0.75) == 'green'
    assert _qos_label(1.0)  == 'green'


def test_qos_label_yellow():
    """Score between 0.50 and 0.75 → yellow"""
    assert _qos_label(0.50) == 'yellow'
    assert _qos_label(0.74) == 'yellow'


def test_qos_label_red():
    """Score below 0.50 → red"""
    assert _qos_label(0.49) == 'red'
    assert _qos_label(0.0)  == 'red'