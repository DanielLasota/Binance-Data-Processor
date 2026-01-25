from enum import Enum


class CommandsRegistry(Enum):
    SHUTDOWN = 'shutdown'

    MODIFY_SUBSCRIPTION = 'modify_subscription'
    OVERRIDE_CONFIG_INTERVAL = 'override_interval'
    SHOW_CONFIG = 'show_config'
    SHOW_STATUS = 'show_status'
    SHOW_IMPORTED_MODULES = 'show_imported_modules'
    GC_COLLECT = 'gc_collect'
