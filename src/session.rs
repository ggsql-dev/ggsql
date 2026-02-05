//! Session management for ggsql-rest
//!
//! Sessions provide isolation for uploaded tables. Each session:
//! - Has a unique UUID
//! - Tracks tables it owns (display names)
//! - Has a last-activity timestamp for timeout cleanup
//!
//! Tables are stored internally as `s_{sessionId}_{tableName}` but the
//! API presents clean names without prefixes.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// A session containing uploaded tables
#[derive(Debug)]
pub struct Session {
    /// Unique session identifier
    pub id: String,
    /// Display names of tables owned by this session
    pub tables: HashSet<String>,
    /// Last activity timestamp for timeout tracking
    pub last_activity: Instant,
}

impl Session {
    /// Create a new session with a generated UUID
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4().to_string().replace("-", "")[..12].to_string(),
            tables: HashSet::new(),
            last_activity: Instant::now(),
        }
    }

    /// Get the internal (prefixed) table name for a display name
    pub fn internal_table_name(&self, display_name: &str) -> String {
        format!("s_{}_{}", self.id, display_name)
    }

    /// Check if this session owns a table (by display name)
    pub fn owns_table(&self, display_name: &str) -> bool {
        self.tables.contains(display_name)
    }

    /// Update last activity timestamp
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Check if session has expired
    pub fn is_expired(&self, timeout: Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

/// Manages all active sessions
pub struct SessionManager {
    /// Active sessions indexed by session ID
    sessions: RwLock<HashMap<String, Session>>,
    /// Session inactivity timeout
    timeout: Duration,
}

impl SessionManager {
    /// Create a new session manager with the specified timeout
    pub fn new(timeout_minutes: u64) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            timeout: Duration::from_secs(timeout_minutes * 60),
        }
    }

    /// Create a new session and return its ID
    pub fn create_session(&self) -> String {
        let session = Session::new();
        let id = session.id.clone();
        let mut sessions = self.sessions.write().unwrap();
        sessions.insert(id.clone(), session);
        id
    }

    /// Get a session by ID, updating its last activity
    pub fn get_session(&self, id: &str) -> Option<Session> {
        let sessions = self.sessions.read().unwrap();
        sessions.get(id).map(|s| Session {
            id: s.id.clone(),
            tables: s.tables.clone(),
            last_activity: s.last_activity,
        })
    }

    /// Touch a session (update last activity)
    pub fn touch_session(&self, id: &str) -> bool {
        let mut sessions = self.sessions.write().unwrap();
        if let Some(session) = sessions.get_mut(id) {
            session.touch();
            true
        } else {
            false
        }
    }

    /// Register a table as belonging to a session
    pub fn register_table(&self, session_id: &str, display_name: &str) -> bool {
        let mut sessions = self.sessions.write().unwrap();
        if let Some(session) = sessions.get_mut(session_id) {
            session.tables.insert(display_name.to_string());
            session.touch();
            true
        } else {
            false
        }
    }

    /// Check if a session owns a specific table
    pub fn session_owns_table(&self, session_id: &str, display_name: &str) -> bool {
        let sessions = self.sessions.read().unwrap();
        sessions
            .get(session_id)
            .map(|s| s.owns_table(display_name))
            .unwrap_or(false)
    }

    /// Get the internal table name for a session's table
    pub fn get_internal_table_name(&self, session_id: &str, display_name: &str) -> Option<String> {
        let sessions = self.sessions.read().unwrap();
        sessions
            .get(session_id)
            .map(|s| s.internal_table_name(display_name))
    }

    /// Delete a session and return its table names (for cleanup)
    pub fn delete_session(&self, id: &str) -> Option<Vec<String>> {
        let mut sessions = self.sessions.write().unwrap();
        sessions.remove(id).map(|s| {
            s.tables
                .iter()
                .map(|name| s.internal_table_name(name))
                .collect()
        })
    }

    /// Get all expired sessions for cleanup
    pub fn get_expired_sessions(&self) -> Vec<(String, Vec<String>)> {
        let sessions = self.sessions.read().unwrap();
        sessions
            .iter()
            .filter(|(_, s)| s.is_expired(self.timeout))
            .map(|(id, s)| {
                let tables: Vec<String> = s
                    .tables
                    .iter()
                    .map(|name| s.internal_table_name(name))
                    .collect();
                (id.clone(), tables)
            })
            .collect()
    }

    /// Remove expired sessions from the manager (called after tables are dropped)
    pub fn remove_expired(&self) {
        let mut sessions = self.sessions.write().unwrap();
        sessions.retain(|_, s| !s.is_expired(self.timeout));
    }

    /// Check if a session exists
    pub fn session_exists(&self, id: &str) -> bool {
        let sessions = self.sessions.read().unwrap();
        sessions.contains_key(id)
    }

    /// Get session count (for health check)
    pub fn session_count(&self) -> usize {
        let sessions = self.sessions.read().unwrap();
        sessions.len()
    }

    /// Get the timeout duration
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_session_creation() {
        let session = Session::new();
        assert!(!session.id.is_empty());
        assert!(session.tables.is_empty());
    }

    #[test]
    fn test_internal_table_name() {
        let session = Session::new();
        let internal = session.internal_table_name("diamonds");
        assert!(internal.starts_with("s_"));
        assert!(internal.ends_with("_diamonds"));
    }

    #[test]
    fn test_session_manager_create_delete() {
        let manager = SessionManager::new(30);
        let id = manager.create_session();
        assert!(manager.session_exists(&id));

        let tables = manager.delete_session(&id);
        assert!(tables.is_some());
        assert!(!manager.session_exists(&id));
    }

    #[test]
    fn test_session_manager_register_table() {
        let manager = SessionManager::new(30);
        let id = manager.create_session();

        assert!(manager.register_table(&id, "diamonds"));
        assert!(manager.session_owns_table(&id, "diamonds"));
        assert!(!manager.session_owns_table(&id, "other"));

        let internal = manager.get_internal_table_name(&id, "diamonds");
        assert!(internal.is_some());
        assert!(internal.unwrap().ends_with("_diamonds"));
    }

    #[test]
    fn test_session_expiry() {
        let manager = SessionManager::new(0); // 0 minute timeout = immediate expiry
        let id = manager.create_session();

        // Session should be expired immediately (0 timeout)
        sleep(Duration::from_millis(10));

        let expired = manager.get_expired_sessions();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, id);
    }
}
