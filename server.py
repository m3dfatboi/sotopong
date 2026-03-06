"""
SotoPong — Backend v1.3 (tournaments v2: prize modes, bracket persistence, rating)
FastAPI + SQLite

Запуск:
  pip install fastapi uvicorn python-multipart
  python server.py
"""

import sqlite3, os, glob, json
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH      = "sotopong.db"
STATIC_DIR   = "static"
AVATARS_DIR  = "avatars"
INITIAL_ELO  = 1000
K_FACTOR     = 32

# Tournament rating bonuses
TOURNAMENT_RATING = {
    "1st": 50,
    "2nd": 25,
    "3rd": 10,
    "semifinal": 5,   # reached semifinal but lost
    "other": 0,
}

app = FastAPI(title="SotoPong API", version="1.3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
os.makedirs(AVATARS_DIR, exist_ok=True)

# ── Database ──────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS players (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL UNIQUE,
                rating     INTEGER NOT NULL DEFAULT 1000,
                wins       INTEGER NOT NULL DEFAULT 0,
                losses     INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS matches (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                p1        TEXT NOT NULL,
                p2        TEXT NOT NULL,
                p1b       TEXT,
                p2b       TEXT,
                s1        INTEGER NOT NULL,
                s2        INTEGER NOT NULL,
                winner    TEXT NOT NULL,
                d1        INTEGER NOT NULL,
                d2        INTEGER NOT NULL,
                played_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            );
            CREATE TABLE IF NOT EXISTS tournaments (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT    NOT NULL,
                status       TEXT    NOT NULL DEFAULT 'active',
                prize_mode   TEXT    NOT NULL DEFAULT 'winner_takes_all',
                bet_mode     TEXT    NOT NULL DEFAULT 'money',
                winner_name  TEXT,
                second_name  TEXT,
                third_name   TEXT,
                bracket_json TEXT,
                created_at   TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS tournament_players (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id   INTEGER NOT NULL REFERENCES tournaments(id),
                player_name     TEXT    NOT NULL,
                bet             INTEGER NOT NULL DEFAULT 0,
                rating_delta    INTEGER NOT NULL DEFAULT 0,
                finish_place    INTEGER
            );
        """)
        conn.commit()
    # Migrations
    with get_db() as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(matches)").fetchall()]
        if "p1b" not in cols:
            conn.execute("ALTER TABLE matches ADD COLUMN p1b TEXT")
        if "p2b" not in cols:
            conn.execute("ALTER TABLE matches ADD COLUMN p2b TEXT")

        tcols = [r[1] for r in conn.execute("PRAGMA table_info(tournaments)").fetchall()]
        if "prize_mode" not in tcols:
            conn.execute("ALTER TABLE tournaments ADD COLUMN prize_mode TEXT NOT NULL DEFAULT 'winner_takes_all'")
        if "second_name" not in tcols:
            conn.execute("ALTER TABLE tournaments ADD COLUMN second_name TEXT")
        if "third_name" not in tcols:
            conn.execute("ALTER TABLE tournaments ADD COLUMN third_name TEXT")
        if "bracket_json" not in tcols:
            conn.execute("ALTER TABLE tournaments ADD COLUMN bracket_json TEXT")
        if "bet_mode" not in tcols:
            conn.execute("ALTER TABLE tournaments ADD COLUMN bet_mode TEXT NOT NULL DEFAULT 'money'")

        tpcols = [r[1] for r in conn.execute("PRAGMA table_info(tournament_players)").fetchall()]
        if "rating_delta" not in tpcols:
            conn.execute("ALTER TABLE tournament_players ADD COLUMN rating_delta INTEGER NOT NULL DEFAULT 0")
        if "finish_place" not in tpcols:
            conn.execute("ALTER TABLE tournament_players ADD COLUMN finish_place INTEGER")

        conn.commit()

init_db()

# ── Helpers ───────────────────────────────────────────────────────────────────
def calc_elo(ra, rb, sa, sb):
    exp_a = 1 / (1 + 10 ** ((rb - ra) / 400))
    act_a = 1 if sa > sb else (0.5 if sa == sb else 0)
    da = round(K_FACTOR * (act_a - exp_a))
    db = round(K_FACTOR * ((1 - act_a) - (1 - exp_a)))
    return ra + da, rb + db, da, db

def fmt_match(m: dict) -> dict:
    try:
        dt = datetime.fromisoformat(m["played_at"])
        m["date"] = dt.strftime("%d.%m.%Y")
        m["time"] = dt.strftime("%H:%M")
    except Exception:
        m["date"] = m.get("played_at", "")
        m["time"] = ""
    return m

def find_avatar(player_id: int) -> Optional[str]:
    files = glob.glob(os.path.join(AVATARS_DIR, f"{player_id}.*"))
    return files[0] if files else None

def player_to_dict(row) -> dict:
    p = dict(row)
    p["avatar_url"] = f"/api/players/{p['id']}/avatar" if find_avatar(p["id"]) else None
    return p

def get_tournament_dict(conn, tid: int) -> dict:
    t = conn.execute("SELECT * FROM tournaments WHERE id=?", (tid,)).fetchone()
    if not t:
        return None
    d = dict(t)
    rows = conn.execute(
        "SELECT * FROM tournament_players WHERE tournament_id=? ORDER BY id", (tid,)
    ).fetchall()
    d["players"] = [dict(p) for p in rows]
    d["prize_pool"] = sum(p["bet"] for p in d["players"])
    return d


# ── Schemas ───────────────────────────────────────────────────────────────────
class PlayerCreate(BaseModel):
    name: str

class MatchCreate(BaseModel):
    p1_name:  str
    p2_name:  str
    score1:   int
    score2:   int
    p1b_name: Optional[str] = None
    p2b_name: Optional[str] = None

class MatchUpdate(BaseModel):
    p1_name:  str
    p2_name:  str
    score1:   int
    score2:   int
    p1b_name: Optional[str] = None
    p2b_name: Optional[str] = None

class TournamentCreate(BaseModel):
    name: str
    prize_mode: str = "winner_takes_all"
    bet_mode: str = "money"

class TournamentUpdate(BaseModel):
    name: str
    prize_mode: str = "winner_takes_all"
    bet_mode: str = "money"

class TournamentPlayerAdd(BaseModel):
    player_name: str
    bet: int

class TournamentBracketSave(BaseModel):
    bracket_json: str

class TournamentFinish(BaseModel):
    winner_name: str
    second_name: Optional[str] = None
    third_name: Optional[str] = None
    bracket_json: Optional[str] = None
    # Map player_name -> rounds_won (for rating calc)
    rounds_won: Optional[dict] = None


# ── Players ───────────────────────────────────────────────────────────────────
@app.get("/api/players")
def get_players():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM players ORDER BY rating DESC").fetchall()
    return [player_to_dict(r) for r in rows]

@app.post("/api/players", status_code=201)
def create_player(body: PlayerCreate):
    name = body.name.strip()
    if not name:
        raise HTTPException(400, "Имя не может быть пустым")
    with get_db() as conn:
        try:
            conn.execute("INSERT INTO players (name, rating, wins, losses) VALUES (?, ?, 0, 0)", (name, INITIAL_ELO))
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(409, f"Игрок «{name}» уже существует")
        row = conn.execute("SELECT * FROM players WHERE name = ?", (name,)).fetchone()
    return player_to_dict(row)

@app.delete("/api/players/{player_id}")
def delete_player(player_id: int):
    with get_db() as conn:
        player = conn.execute("SELECT * FROM players WHERE id = ?", (player_id,)).fetchone()
        if not player:
            raise HTTPException(404, "Игрок не найден")
        name = player["name"]
        rows = conn.execute(
            "SELECT * FROM matches WHERE p1=? OR p2=? OR p1b=? OR p2b=?",
            (name, name, name, name)
        ).fetchall()
        for row in rows:
            m = dict(row)
            is_2v2 = bool(m.get("p1b"))
            team1_won = m["winner"] == m["p1"]
            pairs = ([(m["p1"], m["d1"], team1_won), (m["p1b"], m["d1"], team1_won),
                      (m["p2"], m["d2"], not team1_won), (m["p2b"], m["d2"], not team1_won)]
                     if is_2v2 else
                     [(m["p1"], m["d1"], m["winner"] == m["p1"]),
                      (m["p2"], m["d2"], m["winner"] == m["p2"])])
            for pname, delta, won in pairs:
                if pname and pname != name:
                    conn.execute(
                        "UPDATE players SET rating=rating-?, wins=wins-?, losses=losses-? WHERE name=?",
                        (delta, 1 if won else 0, 0 if won else 1, pname))
        conn.execute("DELETE FROM matches WHERE p1=? OR p2=? OR p1b=? OR p2b=?", (name, name, name, name))
        conn.execute("DELETE FROM players WHERE id = ?", (player_id,))
        conn.commit()
    av = find_avatar(player_id)
    if av:
        try: os.remove(av)
        except: pass
    return {"ok": True}

# ── Avatar ────────────────────────────────────────────────────────────────────
@app.post("/api/players/{player_id}/avatar")
async def upload_avatar(player_id: int, file: UploadFile = File(...)):
    with get_db() as conn:
        if not conn.execute("SELECT id FROM players WHERE id=?", (player_id,)).fetchone():
            raise HTTPException(404, "Игрок не найден")
    if not (file.content_type or "").startswith("image/"):
        raise HTTPException(400, "Файл должен быть изображением")
    ext = (file.filename or "").rsplit(".", 1)[-1].lower()
    if ext not in {"jpg", "jpeg", "png", "gif", "webp", "avif"}:
        ext = "jpg"
    old = find_avatar(player_id)
    if old:
        try: os.remove(old)
        except: pass
    path = os.path.join(AVATARS_DIR, f"{player_id}.{ext}")
    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(413, "Файл слишком большой (макс. 5 МБ)")
    with open(path, "wb") as f:
        f.write(content)
    return {"ok": True, "avatar_url": f"/api/players/{player_id}/avatar"}

@app.get("/api/players/{player_id}/avatar")
def get_avatar(player_id: int):
    path = find_avatar(player_id)
    if not path:
        raise HTTPException(404, "Аватар не найден")
    return FileResponse(path)

# ── Matches ───────────────────────────────────────────────────────────────────
@app.get("/api/matches")
def get_matches():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM matches ORDER BY id DESC").fetchall()
    return [fmt_match(dict(r)) for r in rows]

@app.post("/api/matches", status_code=201)
def create_match(body: MatchCreate):
    if body.score1 == body.score2:
        raise HTTPException(400, "Ничья не допускается")
    if body.score1 < 0 or body.score2 < 0:
        raise HTTPException(400, "Счёт не может быть отрицательным")
    is_2v2 = bool(body.p1b_name and body.p2b_name)
    with get_db() as conn:
        def gp(name):
            p = conn.execute("SELECT * FROM players WHERE name=?", (name,)).fetchone()
            if not p: raise HTTPException(404, f"Игрок «{name}» не найден")
            return p
        pl1 = gp(body.p1_name); pl2 = gp(body.p2_name)
        winner = body.p1_name if body.score1 > body.score2 else body.p2_name
        team1_won = body.score1 > body.score2
        if is_2v2:
            pl1b = gp(body.p1b_name); pl2b = gp(body.p2b_name)
            avg1 = (pl1["rating"] + pl1b["rating"]) // 2
            avg2 = (pl2["rating"] + pl2b["rating"]) // 2
            _, _, d1, d2 = calc_elo(avg1, avg2, body.score1, body.score2)
            for p, d, won in [(pl1, d1, team1_won), (pl1b, d1, team1_won),
                               (pl2, d2, not team1_won), (pl2b, d2, not team1_won)]:
                conn.execute("UPDATE players SET rating=rating+?, wins=wins+?, losses=losses+? WHERE id=?",
                             (d, 1 if won else 0, 0 if won else 1, p["id"]))
            conn.execute(
                "INSERT INTO matches (p1,p2,p1b,p2b,s1,s2,winner,d1,d2) VALUES (?,?,?,?,?,?,?,?,?)",
                (body.p1_name, body.p2_name, body.p1b_name, body.p2b_name, body.score1, body.score2, winner, d1, d2))
        else:
            _, _, d1, d2 = calc_elo(pl1["rating"], pl2["rating"], body.score1, body.score2)
            conn.execute("UPDATE players SET rating=rating+?, wins=wins+?, losses=losses+? WHERE id=?",
                         (d1, 1 if team1_won else 0, 0 if team1_won else 1, pl1["id"]))
            conn.execute("UPDATE players SET rating=rating+?, wins=wins+?, losses=losses+? WHERE id=?",
                         (d2, 0 if team1_won else 1, 1 if team1_won else 0, pl2["id"]))
            conn.execute(
                "INSERT INTO matches (p1,p2,s1,s2,winner,d1,d2) VALUES (?,?,?,?,?,?,?)",
                (body.p1_name, body.p2_name, body.score1, body.score2, winner, d1, d2))
        conn.commit()
        row = conn.execute("SELECT * FROM matches ORDER BY id DESC LIMIT 1").fetchone()
    return fmt_match(dict(row))

@app.delete("/api/matches/{match_id}")
def delete_match(match_id: int):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM matches WHERE id=?", (match_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Матч не найден")
        m = dict(row)
        is_2v2 = bool(m.get("p1b"))
        team1_won = m["winner"] == m["p1"]
        pairs = ([(m["p1"], m["d1"], team1_won), (m["p1b"], m["d1"], team1_won),
                  (m["p2"], m["d2"], not team1_won), (m["p2b"], m["d2"], not team1_won)]
                 if is_2v2 else
                 [(m["p1"], m["d1"], m["winner"] == m["p1"]),
                  (m["p2"], m["d2"], m["winner"] == m["p2"])])
        for pname, delta, won in pairs:
            if pname:
                conn.execute(
                    "UPDATE players SET rating=rating-?, wins=wins-?, losses=losses-? WHERE name=?",
                    (delta, 1 if won else 0, 0 if won else 1, pname))
        conn.execute("DELETE FROM matches WHERE id=?", (match_id,))
        conn.commit()
    return {"ok": True}

@app.put("/api/matches/{match_id}")
def update_match(match_id: int, body: MatchUpdate):
    if body.score1 == body.score2:
        raise HTTPException(400, "Ничья не допускается")
    if body.score1 < 0 or body.score2 < 0:
        raise HTTPException(400, "Счёт не может быть отрицательным")
    is_2v2_new = bool(body.p1b_name and body.p2b_name)
    with get_db() as conn:
        row = conn.execute("SELECT * FROM matches WHERE id=?", (match_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Матч не найден")
        m = dict(row)
        # Revert old ELO
        is_2v2_old = bool(m.get("p1b"))
        team1_won_old = m["winner"] == m["p1"]
        pairs_old = ([(m["p1"], m["d1"], team1_won_old), (m["p1b"], m["d1"], team1_won_old),
                      (m["p2"], m["d2"], not team1_won_old), (m["p2b"], m["d2"], not team1_won_old)]
                     if is_2v2_old else
                     [(m["p1"], m["d1"], m["winner"] == m["p1"]),
                      (m["p2"], m["d2"], m["winner"] == m["p2"])])
        for pname, delta, won in pairs_old:
            if pname:
                conn.execute(
                    "UPDATE players SET rating=rating-?, wins=wins-?, losses=losses-? WHERE name=?",
                    (delta, 1 if won else 0, 0 if won else 1, pname))
        # Apply new ELO
        def gp(name):
            p = conn.execute("SELECT * FROM players WHERE name=?", (name,)).fetchone()
            if not p: raise HTTPException(404, f"Игрок «{name}» не найден")
            return p
        pl1 = gp(body.p1_name); pl2 = gp(body.p2_name)
        winner = body.p1_name if body.score1 > body.score2 else body.p2_name
        team1_won = body.score1 > body.score2
        if is_2v2_new:
            pl1b = gp(body.p1b_name); pl2b = gp(body.p2b_name)
            avg1 = (pl1["rating"] + pl1b["rating"]) // 2
            avg2 = (pl2["rating"] + pl2b["rating"]) // 2
            _, _, d1, d2 = calc_elo(avg1, avg2, body.score1, body.score2)
            for p, d, won in [(pl1, d1, team1_won), (pl1b, d1, team1_won),
                               (pl2, d2, not team1_won), (pl2b, d2, not team1_won)]:
                conn.execute("UPDATE players SET rating=rating+?, wins=wins+?, losses=losses+? WHERE id=?",
                             (d, 1 if won else 0, 0 if won else 1, p["id"]))
            conn.execute(
                "UPDATE matches SET p1=?,p2=?,p1b=?,p2b=?,s1=?,s2=?,winner=?,d1=?,d2=? WHERE id=?",
                (body.p1_name, body.p2_name, body.p1b_name, body.p2b_name,
                 body.score1, body.score2, winner, d1, d2, match_id))
        else:
            _, _, d1, d2 = calc_elo(pl1["rating"], pl2["rating"], body.score1, body.score2)
            conn.execute("UPDATE players SET rating=rating+?, wins=wins+?, losses=losses+? WHERE id=?",
                         (d1, 1 if team1_won else 0, 0 if team1_won else 1, pl1["id"]))
            conn.execute("UPDATE players SET rating=rating+?, wins=wins+?, losses=losses+? WHERE id=?",
                         (d2, 0 if team1_won else 1, 1 if team1_won else 0, pl2["id"]))
            conn.execute(
                "UPDATE matches SET p1=?,p2=?,p1b=NULL,p2b=NULL,s1=?,s2=?,winner=?,d1=?,d2=? WHERE id=?",
                (body.p1_name, body.p2_name, body.score1, body.score2, winner, d1, d2, match_id))
        conn.commit()
        row = conn.execute("SELECT * FROM matches WHERE id=?", (match_id,)).fetchone()
    return fmt_match(dict(row))

# ── Tournaments ───────────────────────────────────────────────────────────────
@app.get("/api/tournaments")
def get_tournaments():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM tournaments ORDER BY id DESC").fetchall()
        return [get_tournament_dict(conn, r["id"]) for r in rows]

@app.post("/api/tournaments", status_code=201)
def create_tournament(body: TournamentCreate):
    name = body.name.strip()
    if not name:
        raise HTTPException(400, "Название не может быть пустым")
    prize_mode = body.prize_mode if body.prize_mode in ("winner_takes_all", "top3_split") else "winner_takes_all"
    bet_mode = body.bet_mode if body.bet_mode in ("money", "rating") else "money"
    with get_db() as conn:
        cur = conn.execute("INSERT INTO tournaments (name, prize_mode, bet_mode) VALUES (?,?,?)", (name, prize_mode, bet_mode))
        conn.commit()
        return get_tournament_dict(conn, cur.lastrowid)

@app.put("/api/tournaments/{tid}")
def update_tournament(tid: int, body: TournamentUpdate):
    name = body.name.strip()
    if not name:
        raise HTTPException(400, "Название не может быть пустым")
    with get_db() as conn:
        row = conn.execute("SELECT id, status FROM tournaments WHERE id=?", (tid,)).fetchone()
        if not row:
            raise HTTPException(404, "Турнир не найден")
        if row["status"] != "active":
            raise HTTPException(400, "Можно редактировать только активные турниры")
        conn.execute(
            "UPDATE tournaments SET name=?, prize_mode=?, bet_mode=? WHERE id=?",
            (name, body.prize_mode, body.bet_mode, tid))
        conn.commit()
        return get_tournament_dict(conn, tid)

@app.delete("/api/tournaments/{tid}")
def delete_tournament(tid: int):
    with get_db() as conn:
        t = conn.execute("SELECT * FROM tournaments WHERE id=?", (tid,)).fetchone()
        if not t:
            raise HTTPException(404, "Турнир не найден")
        td = dict(t)

        # If tournament is on rating mode and still active, return bets to players
        if td.get("bet_mode") == "rating" and td.get("status") == "active":
            players = conn.execute(
                "SELECT player_name, bet FROM tournament_players WHERE tournament_id=?",
                (tid,)
            ).fetchall()
            for p in players:
                if p["bet"] > 0:
                    conn.execute(
                        "UPDATE players SET rating=rating+? WHERE name=?",
                        (p["bet"], p["player_name"])
                    )

        conn.execute("DELETE FROM tournament_players WHERE tournament_id=?", (tid,))
        conn.execute("DELETE FROM tournaments WHERE id=?", (tid,))
        conn.commit()
    return {"ok": True}

@app.post("/api/tournaments/{tid}/players", status_code=201)
def add_tournament_player(tid: int, body: TournamentPlayerAdd):
    if body.bet < 0:
        raise HTTPException(400, "Ставка не может быть отрицательной")
    with get_db() as conn:
        t = conn.execute("SELECT * FROM tournaments WHERE id=?", (tid,)).fetchone()
        if not t:
            raise HTTPException(404, "Турнир не найден")
        td = dict(t)
        if td["status"] != "active":
            raise HTTPException(400, "Турнир уже завершён")
        name = body.player_name.strip()
        if conn.execute(
            "SELECT id FROM tournament_players WHERE tournament_id=? AND player_name=?",
            (tid, name)
        ).fetchone():
            raise HTTPException(409, f"Игрок «{name}» уже в турнире")

        # If tournament is on rating mode, deduct bet from player's rating
        if td.get("bet_mode") == "rating" and body.bet > 0:
            player = conn.execute("SELECT rating FROM players WHERE name=?", (name,)).fetchone()
            if not player:
                raise HTTPException(404, f"Игрок «{name}» не найден")
            if player["rating"] < body.bet:
                raise HTTPException(400, f"Недостаточно рейтинга (есть {player['rating']}, нужно {body.bet})")
            conn.execute("UPDATE players SET rating=rating-? WHERE name=?", (body.bet, name))

        conn.execute(
            "INSERT INTO tournament_players (tournament_id, player_name, bet) VALUES (?,?,?)",
            (tid, name, body.bet)
        )
        conn.commit()
        return get_tournament_dict(conn, tid)

@app.delete("/api/tournaments/{tid}/players/{pid}")
def remove_tournament_player(tid: int, pid: int):
    with get_db() as conn:
        t = conn.execute("SELECT * FROM tournaments WHERE id=?", (tid,)).fetchone()
        if not t:
            raise HTTPException(404, "Турнир не найден")
        td = dict(t)
        if td["status"] != "active":
            raise HTTPException(400, "Нельзя изменить завершённый турнир")

        player_row = conn.execute(
            "SELECT player_name, bet FROM tournament_players WHERE id=? AND tournament_id=?",
            (pid, tid)
        ).fetchone()
        if not player_row:
            raise HTTPException(404, "Игрок не найден")

        # If tournament is on rating mode, return bet to player's rating
        if td.get("bet_mode") == "rating" and player_row["bet"] > 0:
            conn.execute(
                "UPDATE players SET rating=rating+? WHERE name=?",
                (player_row["bet"], player_row["player_name"])
            )

        conn.execute("DELETE FROM tournament_players WHERE id=?", (pid,))
        conn.commit()
    return {"ok": True}

@app.post("/api/tournaments/{tid}/bracket")
def save_bracket(tid: int, body: TournamentBracketSave):
    """Save bracket state without finishing the tournament."""
    with get_db() as conn:
        t = conn.execute("SELECT status FROM tournaments WHERE id=?", (tid,)).fetchone()
        if not t:
            raise HTTPException(404, "Турнир не найден")
        conn.execute("UPDATE tournaments SET bracket_json=? WHERE id=?", (body.bracket_json, tid))
        conn.commit()
    return {"ok": True}

@app.post("/api/tournaments/{tid}/finish")
def finish_tournament(tid: int, body: TournamentFinish):
    with get_db() as conn:
        t = conn.execute("SELECT * FROM tournaments WHERE id=?", (tid,)).fetchone()
        if not t:
            raise HTTPException(404, "Турнир не найден")
        td = dict(t)
        if td["status"] != "active":
            raise HTTPException(400, "Турнир уже завершён")
        if not conn.execute(
            "SELECT id FROM tournament_players WHERE tournament_id=? AND player_name=?",
            (tid, body.winner_name)
        ).fetchone():
            raise HTTPException(400, "Победитель не найден в турнире")

        # Calculate rating deltas for all players
        players_rows = conn.execute(
            "SELECT * FROM tournament_players WHERE tournament_id=?", (tid,)
        ).fetchall()
        rounds_won = body.rounds_won or {}

        # Determine places and rating deltas
        place_map = {}
        if body.winner_name:
            place_map[body.winner_name] = 1
        if body.second_name:
            place_map[body.second_name] = 2
        if body.third_name:
            place_map[body.third_name] = 3

        rating_changes = {}
        bet_mode = td.get("bet_mode", "money")
        prize_mode = td.get("prize_mode", "winner_takes_all")
        prize_pool = td.get("prize_pool", 0)

        for p in players_rows:
            name = p["player_name"]
            place = place_map.get(name)
            rw = rounds_won.get(name, 0)

            # Tournament performance rating (always applied)
            if place == 1:
                delta = TOURNAMENT_RATING["1st"]
            elif place == 2:
                delta = TOURNAMENT_RATING["2nd"]
            elif place == 3:
                delta = TOURNAMENT_RATING["3rd"]
            elif rw >= 2:
                delta = TOURNAMENT_RATING["semifinal"]
            elif rw == 0:
                # Didn't pass first stage - penalty
                delta = -15
            else:
                delta = TOURNAMENT_RATING["other"]

            # Prize pool distribution (only for rating mode)
            prize_delta = 0
            if bet_mode == "rating" and prize_pool > 0:
                if prize_mode == "winner_takes_all":
                    if place == 1:
                        prize_delta = prize_pool
                elif prize_mode == "top3_split":
                    if place == 1:
                        prize_delta = round(prize_pool * 0.6)
                    elif place == 2:
                        prize_delta = round(prize_pool * 0.25)
                    elif place == 3:
                        prize_delta = round(prize_pool * 0.15)

            # Total rating change
            total_delta = delta + prize_delta
            rating_changes[name] = (total_delta, place)

            # Update player rating (always)
            if total_delta != 0:
                conn.execute("UPDATE players SET rating=rating+? WHERE name=?", (total_delta, name))

            # Update tournament_players record
            conn.execute(
                "UPDATE tournament_players SET rating_delta=?, finish_place=? WHERE tournament_id=? AND player_name=?",
                (total_delta, place, tid, name)
            )

        # Save bracket if provided
        bj = body.bracket_json or td.get("bracket_json")

        # Set finished_at timestamp
        from datetime import datetime
        finished_at = datetime.now().isoformat()

        conn.execute(
            "UPDATE tournaments SET status='finished', winner_name=?, second_name=?, third_name=?, bracket_json=?, finished_at=? WHERE id=?",
            (body.winner_name, body.second_name, body.third_name, bj, finished_at, tid)
        )
        conn.commit()
        return get_tournament_dict(conn, tid)


# ── Frontend ──────────────────────────────────────────────────────────────────
if os.path.isdir(STATIC_DIR):
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    print("🏓 SotoPong запущен: http://localhost:8000")
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
