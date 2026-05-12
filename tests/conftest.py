"""Shared pytest fixtures for pipeline tests."""

import pytest
import pandas as pd


@pytest.fixture
def sample_bronze():
    """
    Minimal but realistic Bronze DataFrame covering all test cases:
    - viewer_1: 3 heartbeats, quality switches, some buffering
    - viewer_2: 3 heartbeats, no buffering, stable quality
    - viewer_3: 1 heartbeat, empty qualityDistribution
    """
    return pd.DataFrame([
        # viewer_1 — first heartbeat (window_start should be NULL)
        {
            'customerId': 'customer-1',
            'contentId':  'content-1',
            'clientId':   'viewer-1',
            'timestampInfo': {'server': 1000000, 'agent': 999980},
            'player': {'bufferings': 2, 'bufferingTime': 5000},
            'totalDistribution': {
                'sourceTraffic': {
                    'requests': 5, 'responses': 5.0,
                    'requestedData': 10000, 'receivedData': 10000
                },
                'p2pTraffic': {
                    'requests': 2, 'responses': 2.0,
                    'requestedData': 4000, 'receivedData': 4000
                }
            },
            'qualityDistribution': [
                ('1080p', {
                    'sourceTraffic': {
                        'requests': 3, 'responses': 3.0,
                        'requestedData': 6000, 'receivedData': 6000
                    },
                    'p2pTraffic': {
                        'requests': 1, 'responses': 1.0,
                        'requestedData': 2000, 'receivedData': 2000
                    }
                })
            ],
            'eventDate': '2025-11-13'
        },
        # viewer_1 — second heartbeat (window_start = 1000000)
        {
            'customerId': 'customer-1',
            'contentId':  'content-1',
            'clientId':   'viewer-1',
            'timestampInfo': {'server': 1030000, 'agent': 1029980},
            'player': {'bufferings': 1, 'bufferingTime': 2000},
            'totalDistribution': {
                'sourceTraffic': {
                    'requests': 4, 'responses': 4.0,
                    'requestedData': 8000, 'receivedData': 8000
                },
                'p2pTraffic': {
                    'requests': 0, 'responses': 0.0,
                    'requestedData': 0, 'receivedData': 0
                }
            },
            'qualityDistribution': [
                ('720p', {
                    'sourceTraffic': {
                        'requests': 4, 'responses': 4.0,
                        'requestedData': 8000, 'receivedData': 8000
                    },
                    'p2pTraffic': {
                        'requests': 0, 'responses': 0.0,
                        'requestedData': 0, 'receivedData': 0
                    }
                })
            ],
            'eventDate': '2025-11-13'
        },
        # viewer_1 — third heartbeat (window_start = 1030000)
        {
            'customerId': 'customer-1',
            'contentId':  'content-1',
            'clientId':   'viewer-1',
            'timestampInfo': {'server': 1060000, 'agent': 1059980},
            'player': {'bufferings': 0, 'bufferingTime': 0},
            'totalDistribution': {
                'sourceTraffic': {
                    'requests': 3, 'responses': 3.0,
                    'requestedData': 6000, 'receivedData': 6000
                },
                'p2pTraffic': {
                    'requests': 0, 'responses': 0.0,
                    'requestedData': 0, 'receivedData': 0
                }
            },
            'qualityDistribution': [
                ('1080p', {
                    'sourceTraffic': {
                        'requests': 3, 'responses': 3.0,
                        'requestedData': 6000, 'receivedData': 6000
                    },
                    'p2pTraffic': {
                        'requests': 0, 'responses': 0.0,
                        'requestedData': 0, 'receivedData': 0
                    }
                })
            ],
            'eventDate': '2025-11-13'
        },
        # viewer_2 — first heartbeat, no buffering, stable 1080p
        {
            'customerId': 'customer-1',
            'contentId':  'content-1',
            'clientId':   'viewer-2',
            'timestampInfo': {'server': 1000000, 'agent': 999980},
            'player': {'bufferings': 0, 'bufferingTime': 0},
            'totalDistribution': {
                'sourceTraffic': {
                    'requests': 5, 'responses': 5.0,
                    'requestedData': 10000, 'receivedData': 10000
                },
                'p2pTraffic': {
                    'requests': 0, 'responses': 0.0,
                    'requestedData': 0, 'receivedData': 0
                }
            },
            'qualityDistribution': [
                ('1080p', {
                    'sourceTraffic': {
                        'requests': 5, 'responses': 5.0,
                        'requestedData': 10000, 'receivedData': 10000
                    },
                    'p2pTraffic': {
                        'requests': 0, 'responses': 0.0,
                        'requestedData': 0, 'receivedData': 0
                    }
                })
            ],
            'eventDate': '2025-11-13'
        },
        # viewer_2 — second heartbeat
        {
            'customerId': 'customer-1',
            'contentId':  'content-1',
            'clientId':   'viewer-2',
            'timestampInfo': {'server': 1030000, 'agent': 1029980},
            'player': {'bufferings': 0, 'bufferingTime': 0},
            'totalDistribution': {
                'sourceTraffic': {
                    'requests': 5, 'responses': 5.0,
                    'requestedData': 10000, 'receivedData': 10000
                },
                'p2pTraffic': {
                    'requests': 0, 'responses': 0.0,
                    'requestedData': 0, 'receivedData': 0
                }
            },
            'qualityDistribution': [
                ('1080p', {
                    'sourceTraffic': {
                        'requests': 5, 'responses': 5.0,
                        'requestedData': 10000, 'receivedData': 10000
                    },
                    'p2pTraffic': {
                        'requests': 0, 'responses': 0.0,
                        'requestedData': 0, 'receivedData': 0
                    }
                })
            ],
            'eventDate': '2025-11-13'
        },
        # viewer_3 — single heartbeat, empty qualityDistribution
        {
            'customerId': 'customer-1',
            'contentId':  'content-1',
            'clientId':   'viewer-3',
            'timestampInfo': {'server': 1000000, 'agent': 999980},
            'player': {'bufferings': 0, 'bufferingTime': 0},
            'totalDistribution': {
                'sourceTraffic': {
                    'requests': 1, 'responses': 1.0,
                    'requestedData': 1000, 'receivedData': 1000
                },
                'p2pTraffic': {
                    'requests': 0, 'responses': 0.0,
                    'requestedData': 0, 'receivedData': 0
                }
            },
            'qualityDistribution': [],  # ← empty
            'eventDate': '2025-11-13'
        },
    ])


@pytest.fixture
def silver_sessions(sample_bronze):
    """Silver sessions built from sample bronze — reusable across test files."""
    from src.pipeline.transformations.silver_sessions import build_silver_sessions
    return build_silver_sessions(sample_bronze)


@pytest.fixture
def silver_quality(sample_bronze):
    """Silver quality built from sample bronze — reusable across test files."""
    from src.pipeline.transformations.silver_quality import build_silver_quality
    return build_silver_quality(sample_bronze)


@pytest.fixture
def default_qos_config():
    """Default QoS config with standard thresholds."""
    from src.pipeline.models.schemas import QoSConfig
    return QoSConfig()
