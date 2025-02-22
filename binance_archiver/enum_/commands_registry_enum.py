from enum import Enum


class CommandsRegistry(Enum):
    SHUTDOWN = 'shutdown'

    MODIFY_SUBSCRIPTION = 'modify_subscription'
    OVERRIDE_CONFIG_INTERVAL = 'override_interval'
    SHOW_CONFIG = 'show_config'
    SHOW_STATUS = 'show_status'
    SHOW_IMPORTED_MODULES = 'show_imported_modules'
    GC_COLLECT = 'gc_collect'

    SHOW_TRACEMALLOC_SNAPSHOT_STATISTICS = 'show_tracemalloc_snapshot_statistics'
    SHOW_OBJGRAPH_GROWTH = 'show_objgraph_growth'
    SHOW_PYMPLER_ALL_OBJECTS_ANALYSIS = 'show_pympler_all_objects_analysis'
    SHOW_PYMPLER_DATA_SINK_OBJECT_ANALYSIS = 'show_pympler_data_sink_object_analysis'
    SHOW_PYMPLER_DATA_SINK_OBJECT_ANALYSIS_WITH_DETAIL_LEVEL = 'show_pympler_data_sink_object_analysis_with_detail_level'
    SHOW_PYMPLER_DATA_SINK_OBJECT_ANALYSIS_WITH_MANUAL_ITERATION = 'show_pympler_data_sink_object_analysis_with_manual_iteration'
    SHOW_GUPPY_INFO = 'show_guppy_info'
    ...
