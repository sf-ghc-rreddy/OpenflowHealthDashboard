# Openflow Health Dashboard
A Streamlit-based monitoring dashboard for tracking the health, performance, and operational status of Snowflake Openflow data pipelines

## 🚀 Features

### Mission Control
- **Automated Issue Detection**: Proactive monitoring that identifies critical issues requiring immediate attention
- **High Error Rate Alerts**: Detects runtimes with excessive error rates (>5 errors in 30 minutes)
- **Stopped Processor Detection**: Identifies processors in stopped states across runtimes
- **Action-Oriented Alerts**: Provides specific guidance on next steps for each detected issue

### Pipeline Health Pulse
- **Trend Visualization**: Multi-runtime error and performance trend comparison
- **Time-Based Analysis**: Flexible time window selection (1 hour to 7 days)
- **Queue Time Monitoring**: Track processing delays that could impact SLAs
- **Runtime Comparison**: Side-by-side performance comparison across all user-created runtimes

### Root Cause Analysis
- **Error Ranking**: Top 10 error-producing runtimes for prioritized debugging
- **Drill-Down Workflow**: Step-by-step investigation from runtime → processor → specific errors
- **Advanced Filtering**: Filter by time range, processor, and message content
- **Error Log Explorer**: Browse recent error messages with full context

### Performance Bottleneck Analyzer
- **Backpressure Detection**: Identify connections overwhelmed by data volume
- **Queue Time Analysis**: Find slow processing components causing delays
- **Threshold-Based Filtering**: Customize what constitutes a performance problem
- **Resource Optimization Guidance**: Actionable recommendations for scaling

### Anomaly Tracker
- **Pattern Recognition**: Time-based heatmap showing when errors typically occur
- **Scheduled Job Issues**: Identify problems with recurring processes
- **Day/Hour Analysis**: Weekly and daily error pattern visualization

### Runtime Status
- **Infrastructure Inventory**: Complete list of all known runtimes
- **Activity Monitoring**: Real-time status of active vs inactive runtimes
- **Lifecycle Tracking**: Runtime creation dates and last activity timestamps
- **Cost Optimization**: Identify unused runtimes for decommissioning

## 🛠️ Technology Stack

- **Frontend**: Streamlit with custom CSS theming
- **Data Source**: Snowflake via Snowpark
- **Visualization**: Altair charts with interactive features
- **Data Processing**: Pandas for data manipulation
- **Caching**: Streamlit's built-in caching (60-second TTL)

## 📋 Prerequisites

- Python 3.7+
- Snowflake account with access to Openflow telemetry data and Streamlit



## 📊 Data Requirements

The dashboard expects telemetry data in the following Snowflake table:
Please update `EVENTS_TABLE` with event table name in your account
- **Table**: `openflow.telemetry.events`


## 🔧 Configuration

### Time Windows
- Last Hour (1 hour)
- Last 6 Hours
- Last 24 Hours (default)
- Last 7 Days (168 hours)

### Runtime Filtering
- **All Runtimes**: Show data from all detected runtimes
- **Specific Runtimes**: Multi-select filtering
- **Internal Runtime Toggle**: Option to include/exclude internal system runtimes

### Performance Thresholds
- **Backpressure**: Configurable minimum MiB threshold (default: 0)
- **Queue Time**: Configurable minimum minutes threshold (default: 0)
- **Inactivity**: Configurable hours for runtime status detection (default: 24)

## 🎯 Usage Workflow

### Recommended Investigation Process

1. **Start with Mission Control**: Check for immediate action items
2. **Pipeline Health Pulse**: Identify trending issues across runtimes
3. **Root Cause Analysis**: Drill down into specific errors
4. **Performance Bottleneck Analyzer**: Investigate slow components
5. **Anomaly Tracker**: Look for time-based patterns
6. **Runtime Status**: Verify infrastructure health

### Common Use Cases

- **Daily Health Check**: Quick overview of pipeline status
- **Incident Response**: Rapid identification and resolution of issues
- **Performance Optimization**: Systematic bottleneck identification
- **Capacity Planning**: Runtime utilization and scaling decisions
- **Pattern Analysis**: Understanding recurring operational issues

## 🎨 Customization

### Theme Customization
The dashboard uses a professional light theme defined in the `custom_theme` variable. Modify the CSS to match your organization's brand:

```python
custom_theme = """
<style>
    .stApp {
        background-color: #F0F2F6;  /* Main background */
    }
    /* Add your custom styles here */
</style>
"""
```

### Query Customization
Modify the SQL queries to match your specific telemetry schema or add custom metrics.

## 📈 Performance Considerations

- **Caching**: 60-second TTL on all database queries
- **Query Limits**: Most queries limited to prevent excessive data loading
- **Time Windows**: Larger time windows use day-based aggregation
- **Clear Cache**: Manual cache clearing available in sidebar

## 🔍 Troubleshooting

### Common Issues

**Snowflake Connection Errors**
- Verify your events table name `EVENTS_TABLE` is correct 
- Validate table permissions

**No Data Displayed**
- Confirm telemetry events exist in the specified time window
- Check runtime filtering settings
- Verify table name and schema in configuration

**Performance Issues**
- Use shorter time windows for faster queries
- Clear cache if data appears stale
- Check Snowflake warehouse size


## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request



## 🔄 Version History

- **v1.0.0**: Initial release with core monitoring features
- Multi-tab dashboard interface
- Real-time error detection and alerting
- Performance bottleneck analysis
- Anomaly pattern recognition

---

**Built with ❤️ using Streamlit and Snowflake**
