//! Event filtering implementation

use super::types::*;

/// Event filter for subscriptions
#[derive(Debug, Default, Clone)]
pub enum EventFilter {
    /// Accept all events
    #[default]
    All,
    /// Filter by event type
    ByType(Vec<EventType>),
    /// Filter by priority
    ByPriority(Vec<EventPriority>),
    /// Filter by source
    BySource(Vec<String>),
    /// Filter by tags
    ByTags(Vec<String>),
    /// Custom filter expression
    Custom(FilterExpression),
    /// Composite filter (AND)
    And(Vec<EventFilter>),
    /// Composite filter (OR)
    Or(Vec<EventFilter>),
    /// Negation filter
    Not(Box<EventFilter>),
}

/// Filter expression for custom filtering
#[derive(Debug, Clone)]
pub struct FilterExpression {
    /// Field to filter on
    pub field: String,
    /// Operator to use
    pub operator: FilterOperator,
    /// Value to compare against
    pub value: FilterValue,
}

/// Filter operators
#[derive(Debug, Clone)]
pub enum FilterOperator {
    /// Equal
    Eq,
    /// Not equal
    Ne,
    /// Greater than
    Gt,
    /// Greater than or equal
    Gte,
    /// Less than
    Lt,
    /// Less than or equal
    Lte,
    /// Contains
    Contains,
    /// Starts with
    StartsWith,
    /// Ends with
    EndsWith,
    /// Matches regex
    Regex,
}

/// Filter value types
#[derive(Debug, Clone)]
pub enum FilterValue {
    /// String value
    String(String),
    /// Number value
    Number(f64),
    /// Boolean value
    Bool(bool),
    /// List of strings
    StringList(Vec<String>),
}

impl EventFilter {
    /// Check if an event matches this filter
    pub fn matches(&self, envelope: &EventEnvelope) -> bool {
        match self {
            EventFilter::All => true,

            EventFilter::ByType(types) => types.contains(&envelope.metadata.event_type),

            EventFilter::ByPriority(priorities) => priorities.contains(&envelope.metadata.priority),

            EventFilter::BySource(sources) => {
                sources.iter().any(|s| s == &envelope.metadata.source)
            }

            EventFilter::ByTags(tags) => {
                tags.iter().any(|tag| envelope.metadata.tags.contains(tag))
            }

            EventFilter::Custom(expr) => self.evaluate_expression(expr, envelope),

            EventFilter::And(filters) => filters.iter().all(|f| f.matches(envelope)),

            EventFilter::Or(filters) => filters.iter().any(|f| f.matches(envelope)),

            EventFilter::Not(filter) => !filter.matches(envelope),
        }
    }

    /// Evaluate a custom filter expression
    fn evaluate_expression(&self, expr: &FilterExpression, envelope: &EventEnvelope) -> bool {
        // Get field value
        let field_value = match expr.field.as_str() {
            "event_type" => Some(format!("{:?}", envelope.metadata.event_type)),
            "priority" => Some(format!("{:?}", envelope.metadata.priority)),
            "source" => Some(envelope.metadata.source.clone()),
            _ => None,
        };

        match (&field_value, &expr.value, &expr.operator) {
            (Some(field), FilterValue::String(value), FilterOperator::Eq) => field == value,
            (Some(field), FilterValue::String(value), FilterOperator::Ne) => field != value,
            (Some(field), FilterValue::String(value), FilterOperator::Contains) => {
                field.contains(value)
            }
            (Some(field), FilterValue::String(value), FilterOperator::StartsWith) => {
                field.starts_with(value)
            }
            (Some(field), FilterValue::String(value), FilterOperator::EndsWith) => {
                field.ends_with(value)
            }
            (Some(field), FilterValue::StringList(values), FilterOperator::Contains) => {
                values.iter().any(|v| field.contains(v))
            }
            _ => false,
        }
    }

    /// Create a filter for specific event types
    pub fn by_types(types: &[EventType]) -> Self {
        EventFilter::ByType(types.to_vec())
    }

    /// Create a filter for specific priorities
    pub fn by_priorities(priorities: &[EventPriority]) -> Self {
        EventFilter::ByPriority(priorities.to_vec())
    }

    /// Create a filter for specific sources
    pub fn by_sources(sources: &[String]) -> Self {
        EventFilter::BySource(sources.to_vec())
    }

    /// Create a filter for specific tags
    pub fn by_tags(tags: &[String]) -> Self {
        EventFilter::ByTags(tags.to_vec())
    }

    /// Create a composite AND filter
    pub fn and(filters: Vec<EventFilter>) -> Self {
        EventFilter::And(filters)
    }

    /// Create a composite OR filter
    pub fn or(filters: Vec<EventFilter>) -> Self {
        EventFilter::Or(filters)
    }

    /// Create a NOT filter
    pub fn not(filter: EventFilter) -> Self {
        EventFilter::Not(Box::new(filter))
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;
    use uuid::Uuid;

    fn create_test_envelope(event_type: EventType, priority: EventPriority) -> EventEnvelope {
        EventEnvelope {
            metadata: EventMetadata {
                id: Uuid::new_v4(),
                timestamp: SystemTime::now(),
                event_type,
                priority,
                source: "test".to_string(),
                correlation_id: None,
                tags: vec!["test".to_string()],
                synchronous: false,
            },
            event: Event::Custom {
                event_type: "test".to_string(),
                payload: serde_json::Value::Null,
            },
        }
    }

    #[test]
    fn test_type_filter() {
        let filter = EventFilter::by_types(&[EventType::Stream, EventType::Group]);

        let stream_event = create_test_envelope(EventType::Stream, EventPriority::Normal);
        assert!(filter.matches(&stream_event));

        let node_event = create_test_envelope(EventType::Node, EventPriority::Normal);
        assert!(!filter.matches(&node_event));
    }

    #[test]
    fn test_composite_filters() {
        // AND filter
        let and_filter = EventFilter::and(vec![
            EventFilter::by_types(&[EventType::Stream]),
            EventFilter::by_priorities(&[EventPriority::High]),
        ]);

        let high_stream = create_test_envelope(EventType::Stream, EventPriority::High);
        assert!(and_filter.matches(&high_stream));

        let low_stream = create_test_envelope(EventType::Stream, EventPriority::Low);
        assert!(!and_filter.matches(&low_stream));

        // OR filter
        let or_filter = EventFilter::or(vec![
            EventFilter::by_types(&[EventType::Stream]),
            EventFilter::by_priorities(&[EventPriority::Critical]),
        ]);

        let stream = create_test_envelope(EventType::Stream, EventPriority::Low);
        assert!(or_filter.matches(&stream));

        let critical_node = create_test_envelope(EventType::Node, EventPriority::Critical);
        assert!(or_filter.matches(&critical_node));
    }

    #[test]
    fn test_not_filter() {
        let not_stream = EventFilter::not(EventFilter::by_types(&[EventType::Stream]));

        let stream = create_test_envelope(EventType::Stream, EventPriority::Normal);
        assert!(!not_stream.matches(&stream));

        let node = create_test_envelope(EventType::Node, EventPriority::Normal);
        assert!(not_stream.matches(&node));
    }
}
