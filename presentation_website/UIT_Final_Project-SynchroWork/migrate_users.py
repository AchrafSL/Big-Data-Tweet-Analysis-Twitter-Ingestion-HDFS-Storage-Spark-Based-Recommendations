"""
migrate_users.py - Migration des comptes utilisateurs
Initialise les utilisateurs par défaut et importe les comptes depuis les données MySQL vers SQLite.
"""
from db_init import get_connection
from datetime import datetime

USERS = [
    (2, 'achraf.salimi@uit.ac.ma', 'Salimi@01', 'Salimi', 'UIT', 'verified', 'admin',
     '2024-04-25 14:32:16', '+212611223344', 'achraf.salimi@uit.ac.ma_Salimi.png'),
    (3, 'lilnavincisa2022@gmail.com', 'Salimi@01', 'Linanvinci', 'SL', 'unverified', 'employee',
     '2024-04-28 16:33:40', '', 'lilnavincisa2022@gmail.com_Linanvinci.png'),
    (8, 'thenewachraf2020@gmail.com', 'dadadqdq01@A', 'Salimi', 'Salimiiiiii', 'unverified', 'client',
     '2024-05-30 14:38:09', '0676384393', 'thenewachraf2020@gmail.com_Salimi.png'),
    (9, 'dkhadhak2022@gmail.com', '2022@gmailA', 'dakdha', 'kalfkla', 'unverified', 'client',
     '2024-05-30 14:46:11', '0676384393', 'user583abc_1649114257.png'),
    (10, 'dadagf@gmail.com', 'Aqqq22##@', 'dafa', 'gaga', 'unverified', 'client',
     '2024-05-30 14:48:27', '+212676384393', 'user583abc_1649114257.png'),
    (13, 'iroh25fi@gmail.com', '12345678aA!', 'iroh', 'Mekawi', 'unverified', 'client',
     '2025-01-22 16:18:54', '+12689814810414', 'user583abc_1649114257.png'),
    (14, 'iroh25fire@gmail.com', 'Aa123~1!11kj', 'fafas', 'rwqreq', 'unverified', 'client',
     '2025-01-22 16:19:49', '', 'user583abc_1649114257.png'),
]

def migrate():
    conn = get_connection()
    inserted = 0
    skipped  = 0
    try:
        for u in USERS:
            idCli, email, password, first, last, status, role, date_joined, whatsapp, pfp = u
            existing = conn.execute(
                "SELECT idCli FROM user WHERE email=?", (email,)
            ).fetchone()
            if existing:
                skipped += 1
                continue
            conn.execute(
                "INSERT OR IGNORE INTO user (idCli, Email, Password, FirstName, LastName, "
                "status, role, Date_Joined, whatsapp, pfpName) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (idCli, email, password, first, last, status, role, date_joined, whatsapp, pfp)
            )
            # Create cart for each user
            conn.execute(
                "INSERT OR IGNORE INTO cart (idCli) VALUES (?)", (idCli,)
            )
            inserted += 1
        conn.commit()
        print(f"Migration complete: {inserted} inserted, {skipped} skipped.")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
