use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Note {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub text: String,
    pub source_name: Option<String>,
    pub language: Option<String>,
    pub duration: Option<f64>,
    pub meta: Option<String>,
    pub parent_id: Option<String>,
    #[serde(default)]
    pub position: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSession {
    pub id: String,
    pub title: String,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub source_language: Option<String>,
    pub target_language: Option<String>,
    pub duration_ms: i64,
    pub segment_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSegment {
    pub id: String,
    pub session_id: String,
    pub sequence: i64,
    pub start_ms: i64,
    pub source_text: String,
    pub translated_text: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LiveSessionDetail {
    #[serde(flatten)]
    pub session: LiveSession,
    pub segments: Vec<LiveSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Draft {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub content: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DraftWithNotes {
    #[serde(flatten)]
    pub draft: Draft,
    pub note_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteLink {
    pub id: String,
    pub from_note_id: String,
    pub to_note_id: String,
    pub label: Option<String>,
    pub created_at: i64,
}

/// A note reachable from a given note via a manual link, enriched with the
/// link's label and traversal direction. Used by the LLM context builder to
/// describe the relationship to the model.
#[derive(Debug, Clone, Serialize)]
pub struct LinkedNote {
    pub id: String,
    pub title: String,
    pub text: String,
    pub label: Option<String>,
    /// "out" if the source note links to this one, "in" if this one links to
    /// the source.
    pub direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Translation {
    pub id: String,
    pub note_id: Option<String>,
    pub source_text: String,
    pub translated_text: String,
    pub source_language: Option<String>,
    pub target_language: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NoteWithTags {
    #[serde(flatten)]
    pub note: Note,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
    pub kind: String, // "project" | "note" | "tag"
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    pub kind: String, // "contains" | "tagged" | "link"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphData {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug)]
pub struct StoreError(pub String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StoreError {}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        StoreError(error.to_string())
    }
}

pub struct Storage {
    conn: Arc<Mutex<Connection>>,
}

impl Storage {
    pub fn open(path: PathBuf) -> StoreResult<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| StoreError(format!("storage directory create failed: {e}")))?;
        }
        let conn = Connection::open(&path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let storage = Storage {
            conn: Arc::new(Mutex::new(conn)),
        };
        storage.migrate()?;
        storage.ensure_default_project()?;
        Ok(storage)
    }

    fn migrate(&self) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS notes (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                text TEXT NOT NULL,
                source_name TEXT,
                language TEXT,
                duration REAL,
                meta TEXT,
                parent_id TEXT REFERENCES notes(id) ON DELETE SET NULL,
                position INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_notes_project ON notes(project_id);
            -- Note: `idx_notes_parent` / `idx_notes_position` are created below,
            -- AFTER migrations have added any missing columns on older DBs.

            CREATE TABLE IF NOT EXISTS tags (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                UNIQUE(project_id, name)
            );

            CREATE TABLE IF NOT EXISTS note_tags (
                note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
                tag_id TEXT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                PRIMARY KEY (note_id, tag_id)
            );

            CREATE TABLE IF NOT EXISTS drafts (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_drafts_project ON drafts(project_id);

            CREATE TABLE IF NOT EXISTS draft_notes (
                draft_id TEXT NOT NULL REFERENCES drafts(id) ON DELETE CASCADE,
                note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
                position INTEGER NOT NULL,
                PRIMARY KEY (draft_id, note_id)
            );
            CREATE INDEX IF NOT EXISTS idx_draft_notes_draft ON draft_notes(draft_id);

            CREATE TABLE IF NOT EXISTS note_links (
                id TEXT PRIMARY KEY,
                from_note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
                to_note_id TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
                label TEXT,
                created_at INTEGER NOT NULL,
                UNIQUE(from_note_id, to_note_id)
            );
            CREATE INDEX IF NOT EXISTS idx_note_links_from ON note_links(from_note_id);
            CREATE INDEX IF NOT EXISTS idx_note_links_to ON note_links(to_note_id);

            CREATE TABLE IF NOT EXISTS translations (
                id TEXT PRIMARY KEY,
                note_id TEXT REFERENCES notes(id) ON DELETE SET NULL,
                source_text TEXT NOT NULL,
                translated_text TEXT NOT NULL,
                source_language TEXT,
                target_language TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_translations_note ON translations(note_id);

            CREATE TABLE IF NOT EXISTS live_sessions (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                ended_at INTEGER,
                source_language TEXT,
                target_language TEXT,
                duration_ms INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS live_segments (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES live_sessions(id) ON DELETE CASCADE,
                sequence INTEGER NOT NULL,
                start_ms INTEGER NOT NULL,
                source_text TEXT NOT NULL,
                translated_text TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_live_segments_session ON live_segments(session_id, sequence);
            "#,
        )?;

        // Additive migration: older DBs created before parent_id was introduced.
        // ALTER TABLE cannot reference columns unconditionally, so we probe first.
        let has_parent: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('notes') WHERE name = 'parent_id'")?
            .query([])?
            .next()?
            .is_some();
        if !has_parent {
            conn.execute(
                "ALTER TABLE notes ADD COLUMN parent_id TEXT REFERENCES notes(id) ON DELETE SET NULL",
                [],
            )?;
        }

        // Additive migration for `position` (user-controlled ordering).
        let has_position: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('notes') WHERE name = 'position'")?
            .query([])?
            .next()?
            .is_some();
        if !has_position {
            conn.execute(
                "ALTER TABLE notes ADD COLUMN position INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
            // Seed sensible positions based on existing created_at order so the first
            // render matches what the user is used to (newest first).
            conn.execute(
                "UPDATE notes AS n SET position = (
                    SELECT COUNT(*) FROM notes AS m
                    WHERE m.project_id = n.project_id AND m.created_at > n.created_at
                )",
                [],
            )?;
        }

        // Now that parent_id / position are guaranteed to exist, create indexes.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_parent ON notes(parent_id)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notes_position ON notes(project_id, position)",
            [],
        )?;
        Ok(())
    }

    fn ensure_default_project(&self) -> StoreResult<()> {
        let count: i64 = {
            let conn = self.conn.lock().unwrap();
            conn.query_row("SELECT COUNT(*) FROM projects", [], |row| row.get(0))?
        };
        if count == 0 {
            self.create_project("Default", None)?;
        }
        Ok(())
    }

    pub fn create_project(&self, name: &str, description: Option<&str>) -> StoreResult<Project> {
        let id = new_id();
        let now = now_ms();
        {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO projects (id, name, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                params![id, name, description, now, now],
            )?;
        }
        Ok(Project {
            id,
            name: name.to_string(),
            description: description.map(str::to_string),
            created_at: now,
            updated_at: now,
        })
    }

    pub fn update_project(
        &self,
        id: &str,
        name: &str,
        description: Option<&str>,
    ) -> StoreResult<()> {
        let now = now_ms();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE projects SET name = ?, description = ?, updated_at = ? WHERE id = ?",
            params![name, description, now, id],
        )?;
        Ok(())
    }

    pub fn delete_project(&self, id: &str) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM projects WHERE id = ?", params![id])?;
        Ok(())
    }

    pub fn list_projects(&self) -> StoreResult<Vec<Project>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, description, created_at, updated_at FROM projects ORDER BY updated_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Project {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                created_at: row.get(3)?,
                updated_at: row.get(4)?,
            })
        })?;
        let mut projects = Vec::new();
        for row in rows {
            projects.push(row?);
        }
        Ok(projects)
    }

    pub fn get_project(&self, id: &str) -> StoreResult<Option<Project>> {
        let conn = self.conn.lock().unwrap();
        let project = conn
            .query_row(
                "SELECT id, name, description, created_at, updated_at FROM projects WHERE id = ?",
                params![id],
                |row| {
                    Ok(Project {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        description: row.get(2)?,
                        created_at: row.get(3)?,
                        updated_at: row.get(4)?,
                    })
                },
            )
            .optional()?;
        Ok(project)
    }

    pub fn default_project_id(&self) -> StoreResult<String> {
        let conn = self.conn.lock().unwrap();
        let id: Option<String> = conn
            .query_row(
                "SELECT id FROM projects ORDER BY created_at ASC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        id.ok_or_else(|| StoreError("no project exists".to_string()))
    }

    pub fn add_note(&self, note: &Note) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        // Put the new note at position 0 (top of the list) and shift the rest down
        // so the ordering stays stable across inserts.
        conn.execute(
            "UPDATE notes SET position = position + 1 WHERE project_id = ?",
            params![note.project_id],
        )?;
        conn.execute(
            "INSERT INTO notes (id, project_id, title, text, source_name, language, duration, meta, parent_id, position, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
            params![
                note.id,
                note.project_id,
                note.title,
                note.text,
                note.source_name,
                note.language,
                note.duration,
                note.meta,
                note.parent_id,
                note.created_at,
                note.updated_at,
            ],
        )?;
        conn.execute(
            "UPDATE projects SET updated_at = ? WHERE id = ?",
            params![note.updated_at, note.project_id],
        )?;
        Ok(())
    }

    /// Persist a user-specified order for the given project.
    /// `note_ids` must contain all notes of that project in the desired order.
    pub fn reorder_notes(&self, project_id: &str, note_ids: &[String]) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        for (index, id) in note_ids.iter().enumerate() {
            tx.execute(
                "UPDATE notes SET position = ? WHERE id = ? AND project_id = ?",
                params![index as i64, id, project_id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Update the parent of a note. Prevents self-referencing and simple cycles
    /// (A→B→A). For deeper cycles we rely on UI discipline.
    pub fn set_note_parent(&self, note_id: &str, parent_id: Option<&str>) -> StoreResult<()> {
        if let Some(pid) = parent_id {
            if pid == note_id {
                return Err(StoreError("a note cannot be its own parent".to_string()));
            }
            // Walk up the chain from the candidate parent — if we hit note_id we'd create a cycle.
            let conn = self.conn.lock().unwrap();
            let mut cur = Some(pid.to_string());
            let mut depth = 0usize;
            while let Some(id) = cur {
                if id == note_id {
                    return Err(StoreError("cycle detected in parent chain".to_string()));
                }
                depth += 1;
                if depth > 64 {
                    return Err(StoreError("parent chain too deep".to_string()));
                }
                cur = conn
                    .query_row(
                        "SELECT parent_id FROM notes WHERE id = ?",
                        params![id],
                        |row| row.get::<_, Option<String>>(0),
                    )
                    .optional()?
                    .flatten();
            }
            conn.execute(
                "UPDATE notes SET parent_id = ?, updated_at = ? WHERE id = ?",
                params![pid, now_ms(), note_id],
            )?;
        } else {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "UPDATE notes SET parent_id = NULL, updated_at = ? WHERE id = ?",
                params![now_ms(), note_id],
            )?;
        }
        Ok(())
    }

    pub fn update_note(
        &self,
        id: &str,
        title: &str,
        text: &str,
        meta: Option<&str>,
    ) -> StoreResult<()> {
        let now = now_ms();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE notes SET title = ?, text = ?, meta = ?, updated_at = ? WHERE id = ?",
            params![title, text, meta, now, id],
        )?;
        Ok(())
    }

    pub fn delete_note(&self, id: &str) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM notes WHERE id = ?", params![id])?;
        Ok(())
    }

    pub fn list_notes(&self, project_id: &str) -> StoreResult<Vec<NoteWithTags>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, project_id, title, text, source_name, language, duration, meta, parent_id, position, created_at, updated_at
             FROM notes WHERE project_id = ? ORDER BY position ASC, created_at DESC",
        )?;
        let note_rows = stmt.query_map(params![project_id], |row| {
            Ok(Note {
                id: row.get(0)?,
                project_id: row.get(1)?,
                title: row.get(2)?,
                text: row.get(3)?,
                source_name: row.get(4)?,
                language: row.get(5)?,
                duration: row.get(6)?,
                meta: row.get(7)?,
                parent_id: row.get(8)?,
                position: row.get(9)?,
                created_at: row.get(10)?,
                updated_at: row.get(11)?,
            })
        })?;

        let mut notes = Vec::new();
        for row in note_rows {
            let note = row?;
            let tags = self.note_tags(&conn, &note.id)?;
            notes.push(NoteWithTags { note, tags });
        }
        Ok(notes)
    }

    fn note_tags(&self, conn: &Connection, note_id: &str) -> StoreResult<Vec<String>> {
        let mut stmt = conn.prepare(
            "SELECT t.name FROM tags t
             INNER JOIN note_tags nt ON nt.tag_id = t.id
             WHERE nt.note_id = ? ORDER BY t.name",
        )?;
        let rows = stmt.query_map(params![note_id], |row| row.get::<_, String>(0))?;
        let mut tags = Vec::new();
        for row in rows {
            tags.push(row?);
        }
        Ok(tags)
    }

    pub fn set_note_tags(&self, note_id: &str, tag_names: &[String]) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        let project_id: String = conn.query_row(
            "SELECT project_id FROM notes WHERE id = ?",
            params![note_id],
            |row| row.get(0),
        )?;
        conn.execute("DELETE FROM note_tags WHERE note_id = ?", params![note_id])?;
        for name in tag_names {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                continue;
            }
            let tag_id: String = match conn
                .query_row(
                    "SELECT id FROM tags WHERE project_id = ? AND name = ?",
                    params![project_id, trimmed],
                    |row| row.get(0),
                )
                .optional()?
            {
                Some(id) => id,
                None => {
                    let id = new_id();
                    conn.execute(
                        "INSERT INTO tags (id, project_id, name) VALUES (?, ?, ?)",
                        params![id, project_id, trimmed],
                    )?;
                    id
                }
            };
            conn.execute(
                "INSERT OR IGNORE INTO note_tags (note_id, tag_id) VALUES (?, ?)",
                params![note_id, tag_id],
            )?;
        }
        // Cleanup tags no longer referenced
        conn.execute(
            "DELETE FROM tags WHERE id NOT IN (SELECT tag_id FROM note_tags)",
            [],
        )?;
        Ok(())
    }

    pub fn add_translation(&self, translation: &Translation) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO translations (id, note_id, source_text, translated_text, source_language, target_language, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                translation.id,
                translation.note_id,
                translation.source_text,
                translation.translated_text,
                translation.source_language,
                translation.target_language,
                translation.created_at,
            ],
        )?;
        Ok(())
    }

    pub fn list_translations(&self, project_id: &str) -> StoreResult<Vec<Translation>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT t.id, t.note_id, t.source_text, t.translated_text, t.source_language, t.target_language, t.created_at
             FROM translations t
             LEFT JOIN notes n ON n.id = t.note_id
             WHERE n.project_id = ? OR (t.note_id IS NULL)
             ORDER BY t.created_at DESC",
        )?;
        let rows = stmt.query_map(params![project_id], |row| {
            Ok(Translation {
                id: row.get(0)?,
                note_id: row.get(1)?,
                source_text: row.get(2)?,
                translated_text: row.get(3)?,
                source_language: row.get(4)?,
                target_language: row.get(5)?,
                created_at: row.get(6)?,
            })
        })?;
        let mut translations = Vec::new();
        for row in rows {
            translations.push(row?);
        }
        Ok(translations)
    }

    pub fn delete_translation(&self, id: &str) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM translations WHERE id = ?", params![id])?;
        Ok(())
    }

    pub fn clear_translations(&self, project_id: &str) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM translations WHERE id IN (
                SELECT t.id FROM translations t
                LEFT JOIN notes n ON n.id = t.note_id
                WHERE n.project_id = ? OR (t.note_id IS NULL)
            )",
            params![project_id],
        )?;
        Ok(())
    }

    pub fn save_live_session(
        &self,
        session: &LiveSession,
        segments: &[LiveSegment],
    ) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO live_sessions (id, title, started_at, ended_at, source_language, target_language, duration_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                session.id,
                session.title,
                session.started_at,
                session.ended_at,
                session.source_language,
                session.target_language,
                session.duration_ms,
            ],
        )?;
        for seg in segments {
            conn.execute(
                "INSERT INTO live_segments (id, session_id, sequence, start_ms, source_text, translated_text)
                 VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    seg.id,
                    session.id,
                    seg.sequence,
                    seg.start_ms,
                    seg.source_text,
                    seg.translated_text,
                ],
            )?;
        }
        Ok(())
    }

    pub fn list_live_sessions(&self) -> StoreResult<Vec<LiveSession>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT s.id, s.title, s.started_at, s.ended_at, s.source_language, s.target_language,
                    s.duration_ms, COALESCE(COUNT(seg.id), 0)
             FROM live_sessions s
             LEFT JOIN live_segments seg ON seg.session_id = s.id
             GROUP BY s.id
             ORDER BY s.started_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(LiveSession {
                id: row.get(0)?,
                title: row.get(1)?,
                started_at: row.get(2)?,
                ended_at: row.get(3)?,
                source_language: row.get(4)?,
                target_language: row.get(5)?,
                duration_ms: row.get(6)?,
                segment_count: row.get(7)?,
            })
        })?;
        let mut sessions = Vec::new();
        for row in rows {
            sessions.push(row?);
        }
        Ok(sessions)
    }

    pub fn get_live_session(&self, id: &str) -> StoreResult<Option<LiveSessionDetail>> {
        let conn = self.conn.lock().unwrap();
        let session = conn
            .query_row(
                "SELECT id, title, started_at, ended_at, source_language, target_language, duration_ms
                 FROM live_sessions WHERE id = ?",
                params![id],
                |row| {
                    Ok(LiveSession {
                        id: row.get(0)?,
                        title: row.get(1)?,
                        started_at: row.get(2)?,
                        ended_at: row.get(3)?,
                        source_language: row.get(4)?,
                        target_language: row.get(5)?,
                        duration_ms: row.get(6)?,
                        segment_count: 0,
                    })
                },
            )
            .optional()?;
        let Some(mut session) = session else {
            return Ok(None);
        };

        let mut stmt = conn.prepare(
            "SELECT id, session_id, sequence, start_ms, source_text, translated_text
             FROM live_segments WHERE session_id = ? ORDER BY sequence ASC",
        )?;
        let rows = stmt.query_map(params![id], |row| {
            Ok(LiveSegment {
                id: row.get(0)?,
                session_id: row.get(1)?,
                sequence: row.get(2)?,
                start_ms: row.get(3)?,
                source_text: row.get(4)?,
                translated_text: row.get(5)?,
            })
        })?;
        let mut segments = Vec::new();
        for row in rows {
            segments.push(row?);
        }
        session.segment_count = segments.len() as i64;
        Ok(Some(LiveSessionDetail { session, segments }))
    }

    pub fn delete_live_session(&self, id: &str) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM live_sessions WHERE id = ?", params![id])?;
        Ok(())
    }

    pub fn rename_live_session(&self, id: &str, title: &str) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE live_sessions SET title = ? WHERE id = ?",
            params![title, id],
        )?;
        Ok(())
    }

    pub fn create_draft(
        &self,
        project_id: &str,
        title: &str,
        content: &str,
        note_ids: &[String],
    ) -> StoreResult<Draft> {
        let id = new_id();
        let now = now_ms();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO drafts (id, project_id, title, content, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![id, project_id, title, content, now, now],
        )?;
        for (index, note_id) in note_ids.iter().enumerate() {
            conn.execute(
                "INSERT OR IGNORE INTO draft_notes (draft_id, note_id, position) VALUES (?, ?, ?)",
                params![id, note_id, index as i64],
            )?;
        }
        Ok(Draft {
            id,
            project_id: project_id.to_string(),
            title: title.to_string(),
            content: content.to_string(),
            created_at: now,
            updated_at: now,
        })
    }

    pub fn update_draft(&self, id: &str, title: &str, content: &str) -> StoreResult<()> {
        let now = now_ms();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE drafts SET title = ?, content = ?, updated_at = ? WHERE id = ?",
            params![title, content, now, id],
        )?;
        Ok(())
    }

    pub fn delete_draft(&self, id: &str) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM drafts WHERE id = ?", params![id])?;
        Ok(())
    }

    pub fn list_drafts(&self, project_id: &str) -> StoreResult<Vec<DraftWithNotes>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, project_id, title, content, created_at, updated_at
             FROM drafts WHERE project_id = ? ORDER BY updated_at DESC",
        )?;
        let draft_rows = stmt.query_map(params![project_id], |row| {
            Ok(Draft {
                id: row.get(0)?,
                project_id: row.get(1)?,
                title: row.get(2)?,
                content: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
            })
        })?;

        let mut out = Vec::new();
        for row in draft_rows {
            let draft = row?;
            let mut note_stmt = conn.prepare(
                "SELECT note_id FROM draft_notes WHERE draft_id = ? ORDER BY position ASC",
            )?;
            let note_rows =
                note_stmt.query_map(params![draft.id], |row| row.get::<_, String>(0))?;
            let mut note_ids = Vec::new();
            for n in note_rows {
                note_ids.push(n?);
            }
            out.push(DraftWithNotes { draft, note_ids });
        }
        Ok(out)
    }

    pub fn get_draft(&self, id: &str) -> StoreResult<Option<DraftWithNotes>> {
        let conn = self.conn.lock().unwrap();
        let draft = conn
            .query_row(
                "SELECT id, project_id, title, content, created_at, updated_at
                 FROM drafts WHERE id = ?",
                params![id],
                |row| {
                    Ok(Draft {
                        id: row.get(0)?,
                        project_id: row.get(1)?,
                        title: row.get(2)?,
                        content: row.get(3)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                    })
                },
            )
            .optional()?;
        let Some(draft) = draft else {
            return Ok(None);
        };
        let mut stmt = conn
            .prepare("SELECT note_id FROM draft_notes WHERE draft_id = ? ORDER BY position ASC")?;
        let rows = stmt.query_map(params![id], |row| row.get::<_, String>(0))?;
        let mut note_ids = Vec::new();
        for n in rows {
            note_ids.push(n?);
        }
        Ok(Some(DraftWithNotes { draft, note_ids }))
    }

    pub fn set_draft_notes(&self, draft_id: &str, note_ids: &[String]) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM draft_notes WHERE draft_id = ?",
            params![draft_id],
        )?;
        for (index, note_id) in note_ids.iter().enumerate() {
            conn.execute(
                "INSERT OR IGNORE INTO draft_notes (draft_id, note_id, position) VALUES (?, ?, ?)",
                params![draft_id, note_id, index as i64],
            )?;
        }
        Ok(())
    }

    pub fn get_notes_by_ids(&self, note_ids: &[String]) -> StoreResult<Vec<Note>> {
        if note_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap();
        let placeholders = std::iter::repeat("?")
            .take(note_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT id, project_id, title, text, source_name, language, duration, meta, parent_id, position, created_at, updated_at
             FROM notes WHERE id IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql)?;
        let params_vec: Vec<&dyn rusqlite::ToSql> =
            note_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let rows = stmt.query_map(params_vec.as_slice(), |row| {
            Ok(Note {
                id: row.get(0)?,
                project_id: row.get(1)?,
                title: row.get(2)?,
                text: row.get(3)?,
                source_name: row.get(4)?,
                language: row.get(5)?,
                duration: row.get(6)?,
                meta: row.get(7)?,
                parent_id: row.get(8)?,
                position: row.get(9)?,
                created_at: row.get(10)?,
                updated_at: row.get(11)?,
            })
        })?;
        // Preserve requested order
        let mut map: std::collections::HashMap<String, Note> = std::collections::HashMap::new();
        for row in rows {
            let note = row?;
            map.insert(note.id.clone(), note);
        }
        let ordered: Vec<Note> = note_ids.iter().filter_map(|id| map.remove(id)).collect();
        Ok(ordered)
    }

    /// Fetch every note that is reachable from `note_id` via a manual
    /// note-to-note link, in either direction. Each result carries the link
    /// `label` (if any) and a `direction` hint ("out" = this note links TO it,
    /// "in" = the other note links to this one) so callers can describe the
    /// relationship to the LLM (e.g. "this note's source", "cited by").
    pub fn list_linked_notes(&self, note_id: &str) -> StoreResult<Vec<LinkedNote>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT n.id, n.title, n.text, l.label, 'out' as direction
               FROM note_links l
               JOIN notes n ON n.id = l.to_note_id
              WHERE l.from_note_id = ?
             UNION ALL
             SELECT n.id, n.title, n.text, l.label, 'in' as direction
               FROM note_links l
               JOIN notes n ON n.id = l.from_note_id
              WHERE l.to_note_id = ?",
        )?;
        let rows = stmt.query_map(params![note_id, note_id], |row| {
            Ok(LinkedNote {
                id: row.get(0)?,
                title: row.get(1)?,
                text: row.get(2)?,
                label: row.get(3)?,
                direction: row.get(4)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn add_note_link(
        &self,
        from_note_id: &str,
        to_note_id: &str,
        label: Option<&str>,
    ) -> StoreResult<NoteLink> {
        if from_note_id == to_note_id {
            return Err(StoreError("cannot link a note to itself".to_string()));
        }
        let id = new_id();
        let now = now_ms();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO note_links (id, from_note_id, to_note_id, label, created_at)
             VALUES (?, ?, ?, ?, ?)",
            params![id, from_note_id, to_note_id, label, now],
        )?;
        // If the link already existed, fetch the existing row to return its id
        let link = conn.query_row(
            "SELECT id, from_note_id, to_note_id, label, created_at FROM note_links
             WHERE from_note_id = ? AND to_note_id = ?",
            params![from_note_id, to_note_id],
            |row| {
                Ok(NoteLink {
                    id: row.get(0)?,
                    from_note_id: row.get(1)?,
                    to_note_id: row.get(2)?,
                    label: row.get(3)?,
                    created_at: row.get(4)?,
                })
            },
        )?;
        Ok(link)
    }

    pub fn delete_note_link(&self, id: &str) -> StoreResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM note_links WHERE id = ?", params![id])?;
        Ok(())
    }

    pub fn list_note_links(&self, project_id: &str) -> StoreResult<Vec<NoteLink>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT l.id, l.from_note_id, l.to_note_id, l.label, l.created_at
             FROM note_links l
             INNER JOIN notes n ON n.id = l.from_note_id
             WHERE n.project_id = ?
             ORDER BY l.created_at ASC",
        )?;
        let rows = stmt.query_map(params![project_id], |row| {
            Ok(NoteLink {
                id: row.get(0)?,
                from_note_id: row.get(1)?,
                to_note_id: row.get(2)?,
                label: row.get(3)?,
                created_at: row.get(4)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn graph(&self, project_id: &str) -> StoreResult<GraphData> {
        let project = self
            .get_project(project_id)?
            .ok_or_else(|| StoreError("project not found".to_string()))?;
        let notes = self.list_notes(project_id)?;

        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        nodes.push(GraphNode {
            id: format!("project:{}", project.id),
            label: project.name.clone(),
            kind: "project".to_string(),
        });

        let mut tag_nodes: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();

        let note_ids: std::collections::HashSet<String> =
            notes.iter().map(|n| n.note.id.clone()).collect();

        for note_with_tags in &notes {
            let note = &note_with_tags.note;
            let node_id = format!("note:{}", note.id);
            nodes.push(GraphNode {
                id: node_id.clone(),
                label: if note.title.is_empty() {
                    note.source_name
                        .clone()
                        .unwrap_or_else(|| "(untitled)".to_string())
                } else {
                    note.title.clone()
                },
                kind: "note".to_string(),
            });
            // Only connect notes directly to the project when they're top-level.
            // Children are reached via the "parent" edge, so drawing both edges
            // clutters the graph without adding information. If a note has an
            // orphaned parent reference, fall back to the project edge so it
            // doesn't become disconnected.
            let has_visible_parent = note
                .parent_id
                .as_ref()
                .map(|pid| note_ids.contains(pid))
                .unwrap_or(false);
            if !has_visible_parent {
                edges.push(GraphEdge {
                    source: format!("project:{}", project.id),
                    target: node_id.clone(),
                    kind: "contains".to_string(),
                    id: None,
                    label: None,
                });
            }

            for tag in &note_with_tags.tags {
                let tag_node_id = format!("tag:{}", tag);
                tag_nodes
                    .entry(tag_node_id.clone())
                    .or_insert_with(|| tag.clone());
                edges.push(GraphEdge {
                    source: node_id.clone(),
                    target: tag_node_id,
                    kind: "tagged".to_string(),
                    id: None,
                    label: None,
                });
            }
        }

        for (id, label) in tag_nodes {
            nodes.push(GraphNode {
                id,
                label,
                kind: "tag".to_string(),
            });
        }

        // Manual note-to-note links
        for link in self.list_note_links(project_id)? {
            if !note_ids.contains(&link.from_note_id) || !note_ids.contains(&link.to_note_id) {
                continue;
            }
            edges.push(GraphEdge {
                source: format!("note:{}", link.from_note_id),
                target: format!("note:{}", link.to_note_id),
                kind: "link".to_string(),
                id: Some(link.id),
                label: link.label,
            });
        }

        // Parent → child edges (note hierarchy)
        for note_with_tags in &notes {
            if let Some(parent_id) = &note_with_tags.note.parent_id {
                if note_ids.contains(parent_id) {
                    edges.push(GraphEdge {
                        source: format!("note:{}", parent_id),
                        target: format!("note:{}", note_with_tags.note.id),
                        kind: "parent".to_string(),
                        id: None,
                        label: None,
                    });
                }
            }
        }

        Ok(GraphData { nodes, edges })
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
