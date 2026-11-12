"""
app.py — DamsoStream Flask Application
Plateforme e-commerce de services numériques avec intégration des tendances et recommandations.
"""

from flask import (Flask, render_template, redirect, request,
                   jsonify, session, url_for, send_from_directory, flash)
from flask_mail import Mail, Message
from datetime import timedelta, datetime
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
import phonenumbers
import os
import sqlite3
import csv
import json
import re
import requests
import threading

from db_init import get_connection, init_db, determine_product_metadata
from csv_generator import (
    generate_all_csvs, generate_products_csv, generate_users_csv,
    generate_ratings_csv, generate_trends_csv, import_real_trends,
    CSV_OUTPUT_DIR
)

# ─── Load .env if present ────────────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv()

# ─── Flask app ────────────────────────────────────────────────────────────────
UPLOAD_FOLDER   = os.path.join('static', 'Images', 'Users_pfp')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.secret_key = os.getenv("SECRET_KEY", "damsostream-dev-secret-change-me")
app.permanent_session_lifetime = timedelta(hours=24)

# ─── Flask-Mail ───────────────────────────────────────────────────────────────
app.config['MAIL_USERNAME_VERIFICATION']        = os.getenv("MAIL_USERNAME_VERIFICATION", "")
app.config['MAIL_PASSWORD_VERIFICATION']        = os.getenv("MAIL_PASSWORD_VERIFICATION", "")
app.config['MAIL_DEFAULT_SENDER_VERIFICATION']  = os.getenv("MAIL_DEFAULT_SENDER_VERIFICATION", "")
app.config['MAIL_USERNAME_CONTACT']             = os.getenv("MAIL_USERNAME_CONTACT", "")
app.config['MAIL_PASSWORD_CONTACT']             = os.getenv("MAIL_PASSWORD_CONTACT", "")
app.config['MAIL_DEFAULT_SENDER_CONTACT']       = os.getenv("MAIL_DEFAULT_SENDER_CONTACT", "")
app.config['MAIL_SERVER']         = 'smtp.gmail.com'
app.config['MAIL_PORT']           = 465
app.config['MAIL_USE_TLS']        = False
app.config['MAIL_USE_SSL']        = True
app.config['MAIL_USERNAME']       = os.getenv("MAIL_USERNAME", "")
app.config['MAIL_PASSWORD']       = os.getenv("MAIL_PASSWORD", "")
app.config['MAIL_DEFAULT_SENDER'] = os.getenv("MAIL_DEFAULT_SENDER", "")
mail = Mail(app)

# ─── Initialise SQLite database on startup ───────────────────────────────────
with app.app_context():
    init_db()


# ─── Helpers ─────────────────────────────────────────────────────────────────

def db():
    """Return a new SQLite connection with row_factory set."""
    return get_connection()


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def format_whatsapp_number(number, country_code):
    if number.startswith('+'):
        parsed = phonenumbers.parse(number)
        national = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.NATIONAL)
        national = ''.join(filter(str.isdigit, national)).lstrip('0')
    else:
        national = number.lstrip('0')
    full_number = f"+{country_code}{national}"
    parsed = phonenumbers.parse(full_number)
    return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)


def generate_verification_token(email, firstname, lastname, password, operation):
    email_num     = sum(ord(c) for c in email)
    firstname_num = sum(ord(c) for c in firstname)
    lastname_num  = sum(ord(c) for c in lastname)
    password_num  = sum(ord(c) for c in password)
    return int(((email_num + firstname_num + lastname_num + password_num) * operation) / 2)


def is_dev_mode():
    """True when running in development or debug mode."""
    return app.debug or os.getenv("FLASK_ENV") == "development" or os.getenv("DEV_MODE", "true").lower() in ("1", "true")


def send_verification_email(email, token):
    link = f'http://127.0.0.1:5000/verify_email?token={token}&email={email}'
    if is_dev_mode():
        print(f"[DEV_MODE] Email verification link for {email}: {link}")
    msg = Message('Verify Your Email',
                  sender='damsostream.login@gmail.com', recipients=[email])
    msg.html = render_template('Email_template.html', msg='email', verification_link=link)
    try:
        mail.send(msg)
    except Exception as exc:
        if not is_dev_mode():
            print(f"[mail] Failed to send email to {email}: {exc}")


def send_verification_password(email, token):
    link = f'http://127.0.0.1:5000/ResetPassword_Page?token={token}&email={email}'
    if is_dev_mode():
        print(f"[DEV_MODE] Password reset link for {email}: {link}")
    msg = Message('Reset Your Password',
                  sender='damsostream.login@gmail.com', recipients=[email])
    msg.html = render_template('Email_template.html', msg='password', verification_link=link)
    try:
        mail.send(msg)
    except Exception as exc:
        if not is_dev_mode():
            print(f"[mail] Failed to send password reset email to {email}: {exc}")


# ─── Predefined choices for user preferences ─────────────────────────────────
AVAILABLE_THEMES = [
    "Entertainment", "Music", "Sports", "Education", "Productivity",
    "Security", "Social", "Gaming", "Anime", "Design", "Bundle", "Other"
]
AVAILABLE_COLORS = [
    "Red", "Blue", "Green", "Yellow", "Purple", "Orange",
    "Pink", "Black", "White", "Gray", "Gold", "Silver", "Other"
]


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/home', methods=["GET", "POST"])
@app.route('/', methods=["GET", "POST"])
def home():
    return render_template("Home.html")


# ─── Offers ──────────────────────────────────────────────────────────────────
@app.route('/Offers', methods=["GET", "POST"])
def Offers():
    conn = db()
    try:
        total = conn.execute("SELECT COUNT(*) FROM offers").fetchone()[0]
        word  = request.args.get('product')
        if word:
            rows = conn.execute(
                "SELECT name, Offer_price, idOffer, image_Name FROM offers WHERE name LIKE ?",
                (f"%{word}%",)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT name, Offer_price, idOffer, image_Name FROM offers"
            ).fetchall()

        offer_data = [{'name': r[0], 'price': r[1], 'idOffer': r[2], 'image_Name': r[3]}
                      for r in rows]
        offer_data = zip(range(total), offer_data)
    finally:
        conn.close()
    return render_template("Offers.html", OffersNumber=total, OfferData=offer_data, word=word)


# ─── Product description + comments + rating ─────────────────────────────────
@app.route("/Product_Description", methods=["GET"])
def Product_Description():
    pid = request.args.get('productId')
    conn = db()
    try:
        offer_row = conn.execute(
            "SELECT idOffer, description, image_Name, Offer_price, name FROM offers WHERE idOffer=?",
            (pid,)
        ).fetchone()
        if not offer_row:
            return redirect('/Offers')

        offer_data = {
            'productId': offer_row[0], 'description': offer_row[1],
            'image_Name': offer_row[2], 'Offer_price': offer_row[3], 'name': offer_row[4]
        }

        comment_rows = conn.execute(
            "SELECT idCli, comment_TEXT, Date_written, idComment "
            "FROM comment WHERE idOffer=? ORDER BY Date_written DESC LIMIT 100",
            (pid,)
        ).fetchall()

        comments_data = []
        for res in comment_rows:
            usr = conn.execute(
                "SELECT LastName, FirstName, pfpName FROM user WHERE idCli=?",
                (res[0],)
            ).fetchone()
            if usr:
                comments_data.append({
                    'idCli': res[0], 'LastName': usr[0], 'FirstName': usr[1],
                    'comment': res[1], 'Date_written': res[2],
                    'idComment': res[3], 'pfpName': usr[2]
                })

        # Current user's existing rating (if logged in)
        user_rating = None
        if session.get('logged_in') and session.get('idCli'):
            r = conn.execute(
                "SELECT rating FROM ratings WHERE idCli=? AND idOffer=?",
                (session['idCli'], pid)
            ).fetchone()
            user_rating = r[0] if r else None

        # Average rating
        avg_row = conn.execute(
            "SELECT AVG(rating), COUNT(*) FROM ratings WHERE idOffer=?", (pid,)
        ).fetchone()
        avg_rating   = round(avg_row[0], 1) if avg_row[0] else None
        rating_count = avg_row[1]

    finally:
        conn.close()

    return render_template(
        "Product_Description.html",
        offer=offer_data, comments=comments_data,
        user_rating=user_rating, avg_rating=avg_rating, rating_count=rating_count
    )


# ─── Submit comment ───────────────────────────────────────────────────────────
@app.route("/submit_comment", methods=["POST"])
def submit_comment():
    productId = request.form.get("productId")
    idCli     = session.get("idCli")
    comment   = request.form.get("comment")
    date      = datetime.now().isoformat()
    conn = db()
    try:
        conn.execute(
            "INSERT INTO comment (idOffer, idCli, comment_TEXT, Date_written) VALUES (?,?,?,?)",
            (productId, idCli, comment, date)
        )
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for("Product_Description", productId=productId))


# ─── Delete comment ───────────────────────────────────────────────────────────
@app.route('/DeleteComment', methods=["POST"])
def DeleteComment():
    productId = request.form.get("productId")
    idComment = request.form.get("idComment")
    conn = db()
    try:
        conn.execute("DELETE FROM comment WHERE idComment=?", (idComment,))
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for("Product_Description", productId=productId))


# ─── Rate product ─────────────────────────────────────────────────────────────
@app.route("/rate_product", methods=["POST"])
def rate_product():
    if not session.get('logged_in'):
        return jsonify({"error": "Not logged in"}), 401
    pid    = request.form.get("productId")
    rating = request.form.get("rating")
    try:
        rating = int(rating)
        if not 1 <= rating <= 5:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({"error": "Rating must be 1-5"}), 400

    idCli = session.get("idCli")
    now   = datetime.now().isoformat()
    conn  = db()
    try:
        conn.execute(
            "INSERT INTO ratings (idCli, idOffer, rating, rated_at) VALUES (?,?,?,?) "
            "ON CONFLICT(idCli, idOffer) DO UPDATE SET rating=excluded.rating, rated_at=excluded.rated_at",
            (idCli, pid, rating, now)
        )
        conn.commit()
    finally:
        conn.close()
    generate_ratings_csv()
    return redirect(url_for("Product_Description", productId=pid))


# ─── Contact ─────────────────────────────────────────────────────────────────
@app.route('/ContactUS', methods=["GET"])
def ContactUS():
    return render_template("ContactUS.html")


@app.route("/ConstactUSFormEmailSend", methods=["POST"])
def ConstactUSFormEmailSend():
    senderName  = request.form.get("ContactUSName")
    senderEmail = request.form.get("ContactUSEmail")
    senderMSG   = request.form.get("ContactUSMSG")
    email = "damsostream@gmail.com"

    app.config['MAIL_USERNAME']       = app.config['MAIL_USERNAME_CONTACT']
    app.config['MAIL_PASSWORD']       = app.config['MAIL_PASSWORD_CONTACT']
    app.config['MAIL_DEFAULT_SENDER'] = app.config['MAIL_DEFAULT_SENDER_CONTACT']
    mail.init_app(app)

    try:
        msg = Message(senderName + ' Contact request',
                      sender="DamsoStream.ContactUS@gmail.com", recipients=[email])
        msg.body = senderMSG
        msg.reply_to = senderEmail
        mail.send(msg)
    except Exception:
        pass

    app.config['MAIL_USERNAME']       = app.config['MAIL_USERNAME_VERIFICATION']
    app.config['MAIL_PASSWORD']       = app.config['MAIL_PASSWORD_VERIFICATION']
    app.config['MAIL_DEFAULT_SENDER'] = app.config['MAIL_DEFAULT_SENDER_VERIFICATION']
    mail.init_app(app)

    return redirect("ContactUS")


@app.route('/AboutUs', methods=["GET"])
def AboutUS():
    return render_template("AboutUS.html")


# ═══════════════════════════════════════════════════════════════════════════════
# CART
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/AddOfferToCart', methods=["POST"])
def AddOfferToCart():
    if not session.get('logged_in'):
        return redirect('/Login')
    if session.get('role') == 'employee':
        return redirect('home')

    idOffer = request.form.get('idOffer')
    conn = db()
    try:
        cart_row = conn.execute(
            "SELECT idCart FROM cart WHERE idCli=?", (session["idCli"],)
        ).fetchone()
        if not cart_row:
            conn.close()
            return redirect('/Cart')
        idCart = cart_row[0]

        exists = conn.execute(
            "SELECT idCart FROM cartoffers WHERE idCart=? AND idOffer=?",
            (idCart, idOffer)
        ).fetchone()
        if not exists:
            conn.execute(
                "INSERT INTO cartoffers (idCart, idOffer) VALUES (?,?)", (idCart, idOffer)
            )
            conn.commit()
    finally:
        conn.close()
    return redirect("/Cart")


@app.route('/Cart', methods=["GET", "POST"])
def Cart():
    if not session.get('logged_in'):
        return redirect('/Login')
    if not session.get('role'):
        return redirect('/Login')
    if session.get('role') == 'employee':
        return redirect('home')

    conn = db()
    try:
        cart_row = conn.execute(
            "SELECT idCart FROM cart WHERE idCli=?", (session["idCli"],)
        ).fetchone()
        if not cart_row:
            conn.close()
            return render_template("Cart.html", OFFERData=[])
        idCart = cart_row[0]
        session["idCart"] = idCart

        price_row = conn.execute(
            "SELECT SUM(o.Offer_price) FROM cartoffers co "
            "JOIN offers o ON co.idOffer=o.idOffer WHERE co.idCart=?",
            (idCart,)
        ).fetchone()
        fullprice = price_row[0] or 0
        session["fullprice"] = fullprice

        conn.execute("UPDATE cart SET fullPrice=? WHERE idCart=?", (fullprice, idCart))
        conn.commit()

        rows = conn.execute(
            "SELECT co.idOffer, o.description, o.image_Name, o.Offer_price, o.name "
            "FROM cartoffers co JOIN offers o ON co.idOffer=o.idOffer WHERE co.idCart=?",
            (idCart,)
        ).fetchall()

        offers_data = [
            {'idOffer': r[0], 'description': r[1], 'image_Name': r[2],
             'Offer_price': r[3], 'name': r[4]}
            for r in rows
        ]
    finally:
        conn.close()
    return render_template("Cart.html", OFFERData=offers_data)


@app.route('/RemoveOffer_Cart', methods=["POST"])
def RemoveOffer_Cart():
    idOffer = request.form.get('offerId')
    idCart  = session.get("idCart")
    conn = db()
    try:
        conn.execute(
            "DELETE FROM cartoffers WHERE idCart=? AND idOffer=?", (idCart, idOffer)
        )
        conn.commit()
    finally:
        conn.close()
    return redirect("/Cart")


@app.route("/SendOrder", methods=["POST"])
def SendOrder():
    idCart = session.get("idCart")
    conn = db()
    try:
        offers = conn.execute(
            "SELECT idOffer FROM cartoffers WHERE idCart=?", (idCart,)
        ).fetchall()
        if not offers:
            conn.close()
            return redirect("Offers")

        cur = conn.execute(
            "INSERT INTO orders (idCli, TotalPrice) VALUES (?,?)",
            (session["idCli"], session.get("fullprice", 0))
        )
        conn.commit()
        idOrder = cur.lastrowid

        for o in offers:
            conn.execute(
                "INSERT INTO orderoffers (idOrder, idOffer) VALUES (?,?)", (idOrder, o[0])
            )
        conn.commit()

        conn.execute("DELETE FROM cartoffers WHERE idCart=?", (idCart,))
        conn.commit()
    finally:
        conn.close()
    return redirect("home")


# ═══════════════════════════════════════════════════════════════════════════════
# AUTH — SIGNUP
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/Signup', methods=["GET", "POST"])
def Signup():
    return render_template("Signup.html")


@app.route('/CheckEmail', methods=["POST"])
def CheckEmail():
    if request.method != "POST":
        return jsonify({"error": "Invalid request method"})
    email = request.json.get('email', '').lower()
    conn = db()
    try:
        row = conn.execute("SELECT Email FROM user WHERE email=?", (email,)).fetchone()
    finally:
        conn.close()
    return jsonify({"usr_exist": "true" if row else "false"})


@app.route('/SignUpInput', methods=["POST"])
def SignUpInput():
    email     = request.form.get("email", "").lower()
    password  = request.form.get("password")
    firstname = request.form.get("firstname")
    lastname  = request.form.get("lastname")
    whatsapp  = request.form.get("WhatsappNumber", "")
    country   = request.form.get("countryCode", "")

    if whatsapp:
        try:
            whatsapp = format_whatsapp_number(whatsapp, country)
        except Exception:
            pass

    role = request.form.get("role", "client").lower()
    if role not in ("client", "producer"):
        role = "client"

    dev = is_dev_mode()
    initial_status = 'verified' if dev else 'unverified'

    date = datetime.now().isoformat()
    conn = db()
    try:
        cur = conn.execute(
            "INSERT INTO user (FirstName, LastName, Email, Password, Date_Joined, whatsapp, status, role) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (firstname, lastname, email, password, date, whatsapp, initial_status, role)
        )
        conn.commit()
        idCli = cur.lastrowid

        conn.execute("INSERT INTO cart (idCli) VALUES (?)", (idCli,))
        conn.commit()
    finally:
        conn.close()

    token = generate_verification_token(email, firstname, lastname, password, 50)
    send_verification_email(email, token)

    if dev:
        print(f"[DEV_MODE] User {email} registered and auto-verified.")
        return redirect('/Login?registered=1')

    return redirect(f'/Verify?email={email}')


@app.route('/Verify', methods=["GET"])
def Verify():
    email = request.args.get('email')
    return render_template("Login.html", email=email, dev_mode=is_dev_mode())


@app.route('/verify_email', methods=['GET'])
def verify_email():
    try:
        token = int(request.args.get('token'))
    except (TypeError, ValueError):
        return render_template('Signup.html', verif=1, invalidToken=1)

    email = request.args.get('email', '').lower()
    conn  = db()
    try:
        row = conn.execute(
            "SELECT Password, FirstName, LastName, status FROM user WHERE email=?", (email,)
        ).fetchone()
        if not row:
            conn.close()
            return render_template('Signup.html', verif=1, invalidToken=1)
        Password, FirstName, LastName, status = row

        new_token = generate_verification_token(email, FirstName, LastName, Password, 50)
        if token != new_token:
            conn.close()
            return render_template('Signup.html', verif=1, invalidToken=1)

        if status != 'verified':
            conn.execute("UPDATE user SET status='verified' WHERE email=?", (email,))
            conn.commit()
            conn.close()
            return redirect('/Verified_email')
        else:
            conn.close()
            return redirect('/Already_Verified_email')
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.route('/Verified_email', methods=["GET"])
def Verified_email():
    return render_template('Signup.html', verif=0)


@app.route('/Already_Verified_email', methods=["GET"])
def Already_Verified_email():
    return render_template('Signup.html', verif=1)


# ═══════════════════════════════════════════════════════════════════════════════
# AUTH — LOGIN
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/Login', methods=["GET", "POST"])
def Login():
    return render_template("Login.html", x=0)


@app.route('/passwordForgot', methods=["POST", "GET"])
def passwordForgot():
    return render_template("Login.html", x=1, Reset=0)


@app.route('/CheckLogin', methods=["POST"])
def CheckLogin():
    if request.method != "POST":
        return jsonify({"error": "Invalid request method"})

    email  = request.json.get('email', '').lower()
    passwd = request.json.get('passwd', '')

    conn = db()
    try:
        row = conn.execute(
            "SELECT Email, Password, FirstName, LastName, role, idCli, "
            "Date_Joined, whatsapp, pfpName, favTheme, favColor "
            "FROM user WHERE LOWER(email)=? OR LOWER(FirstName)=?",
            (email, email)
        ).fetchone()
    finally:
        conn.close()

    password_valid = False
    if row:
        if row[1] == passwd:
            password_valid = True
        else:
            try:
                password_valid = check_password_hash(row[1], passwd)
            except Exception:
                password_valid = False

    if row and password_valid:
        session.permanent = True
        session["logged_in"] = True
        session["FirstName"]  = row[2]
        session["LastName"]   = row[3]
        session["Email"]      = row[0]
        session["role"]       = row[4]
        session["idCli"]      = row[5]
        session["whatsapp"]   = row[7] or ""
        session["pfpName"]    = row[8]
        session["favTheme"]   = row[9] or ""
        session["favColor"]   = row[10] or ""

        date_joined = row[6]
        if date_joined:
            try:
                dt = datetime.fromisoformat(str(date_joined))
                session["Date_Joined"] = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                session["Date_Joined"] = str(date_joined)
        else:
            session["Date_Joined"] = ""
        return jsonify({"usr_exist": "true"})
    return jsonify({"usr_exist": "false"})


@app.route('/LoginInput', methods=["POST"])
def LoginInput():
    return redirect('/home')


@app.route('/ResetPassword', methods=["POST", "GET"])
def ResetPassword():
    email = (request.form.get("E_mail") if request.method == "POST"
             else request.args.get("E_mail", "")).lower()

    conn = db()
    try:
        row = conn.execute(
            "SELECT Email, Password, FirstName, LastName FROM user WHERE email=?", (email,)
        ).fetchone()
    finally:
        conn.close()

    if row and email == row[0]:
        token = generate_verification_token(email, row[2], row[3], row[1], 60)
        send_verification_password(email, token)
        reset_link = f"http://127.0.0.1:5000/ResetPassword_Page?token={token}&email={email}"
        return render_template(
            'Login.html', x=1, Reset=1, email=email,
            dev_mode=is_dev_mode(),
            dev_reset_link=reset_link if is_dev_mode() else None
        )

    return render_template('Signup.html', verif=1, Reset=-1)


@app.route('/ResetPassword_Page', methods=["GET"])
def ResetPassword_Page():
    try:
        token = int(request.args.get('token'))
    except (TypeError, ValueError):
        return render_template('Signup.html', verif=1, invalidToken=2)

    email = request.args.get('email', '').lower()
    conn  = db()
    try:
        row = conn.execute(
            "SELECT Password, FirstName, LastName FROM user WHERE email=?", (email,)
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return render_template('Signup.html', verif=1, invalidToken=2)

    new_token = generate_verification_token(email, row[1], row[2], row[0], 60)
    if token == new_token:
        return render_template('Login.html', reset_form=1, email=email)
    return render_template('Signup.html', verif=1, invalidToken=2)


@app.route('/CheckPassword', methods=["POST"])
def CheckPassword():
    if request.method != "POST":
        return jsonify({"error": "Invalid request method"})
    email    = request.json.get('email', '')
    password = request.json.get('passwd', '')
    conn = db()
    try:
        row = conn.execute("SELECT Password FROM user WHERE email=?", (email,)).fetchone()
    finally:
        conn.close()
    if row and row[0] == password:
        return jsonify({"pswd_exist": "true"})
    return jsonify({"pswd_exist": "false"})


@app.route('/ResetPassword_Apply', methods=["POST"])
def ResetPassword_Apply():
    email        = request.form.get('Email')
    new_password = request.form.get('Password')
    conn = db()
    try:
        conn.execute("UPDATE user SET Password=? WHERE email=?", (new_password, email))
        conn.commit()
    finally:
        conn.close()
    return render_template('Signup.html', verif=0, apply=1)


@app.route('/ResendLink', methods=["POST"])
def ResendLink():
    email            = request.form.get('EMAIL')
    resend_link_type = request.form.get('ResendLink_Type')
    conn = db()
    try:
        row = conn.execute(
            "SELECT Password, FirstName, LastName FROM user WHERE email=?", (email,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return redirect('/Login')

    if resend_link_type == 'email':
        token = generate_verification_token(email, row[1], row[2], row[0], 50)
        send_verification_email(email, token)
        return redirect(f'/Verify?email={email}')
    else:
        token = generate_verification_token(email, row[1], row[2], row[0], 60)
        send_verification_password(email, token)
        return redirect(f'/ResetPassword?E_mail={email}')


# ═══════════════════════════════════════════════════════════════════════════════
# USER PAGE
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/USER_Page", methods=["GET"])
def USER_Page():
    if not session.get("logged_in"):
        return redirect("/home")
    return render_template(
        "USER_Page.html",
        available_themes=AVAILABLE_THEMES,
        available_colors=AVAILABLE_COLORS
    )


@app.route("/UpdateInfo", methods=["POST"])
def UpdateInfo():
    firstname = request.form.get('firstname')
    lastname  = request.form.get('lastname')
    whatsapp  = request.form.get('whatsappNumber', '')
    email     = request.form.get('email', '').lower()

    if whatsapp:
        try:
            country = request.form.get("CountryCode", "")
            whatsapp = format_whatsapp_number(whatsapp, country)
        except Exception:
            pass

    conn = db()
    try:
        if email != session["Email"]:
            conn.execute(
                "UPDATE user SET status='unverified' WHERE email=?", (session["Email"],)
            )
            conn.commit()
            row = conn.execute(
                "SELECT Password FROM user WHERE email=?", (session["Email"],)
            ).fetchone()

            conn.execute(
                "UPDATE user SET FirstName=?, LastName=?, whatsapp=?, email=? WHERE email=?",
                (firstname, lastname, whatsapp, email, session["Email"])
            )
            conn.commit()

            session.update({"FirstName": firstname, "LastName": lastname,
                            "Email": email, "whatsapp": whatsapp})
            if row:
                token = generate_verification_token(email, firstname, lastname, row[0], 50)
                send_verification_email(email, token)
            conn.close()
            return redirect(f'/Verify?email={email}')
        else:
            conn.execute(
                "UPDATE user SET FirstName=?, LastName=?, whatsapp=?, email=? WHERE email=?",
                (firstname, lastname, whatsapp, email, session["Email"])
            )
            conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    session.update({"FirstName": firstname, "LastName": lastname, "whatsapp": whatsapp})
    return redirect('/USER_Page')


@app.route("/UpdatePreferences", methods=["POST"])
def UpdatePreferences():
    """Update user favTheme and favColor."""
    if not session.get('logged_in'):
        return redirect('/Login')

    fav_theme = request.form.get('favTheme', '').strip()
    fav_color = request.form.get('favColor', '').strip()

    conn = db()
    try:
        conn.execute(
            "UPDATE user SET favTheme=?, favColor=? WHERE idCli=?",
            (fav_theme, fav_color, session["idCli"])
        )
        conn.commit()
    finally:
        conn.close()

    session["favTheme"] = fav_theme
    session["favColor"] = fav_color
    generate_users_csv()
    return redirect('/USER_Page')


# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# CUSTOMER RECOMMENDATIONS ENGINE (Connected to Linux Master API)
# ═══════════════════════════════════════════════════════════════════════════════

RECOMMENDATION_API_BASE_URL = os.getenv("RECOMMENDATION_API_BASE_URL", "http://<LINUX_VM_HOST>:5002")
RECOMMENDATION_REFRESH_TIMEOUT = int(os.getenv("RECOMMENDATION_REFRESH_TIMEOUT", "300"))
RECOMMENDATION_GET_TIMEOUT = int(os.getenv("RECOMMENDATION_GET_TIMEOUT", "15"))

_refresh_lock = threading.Lock()


def fetch_raw_recommendations(base_url, user_id, timeout=RECOMMENDATION_GET_TIMEOUT):
    """
    Fetch already-generated recommendations for a user via GET.
    Returns a list of raw recommendation items if available, or None if not found / empty.
    """
    rec_url = f"{base_url}/api/recommendations/{user_id}"
    resp = requests.get(rec_url, timeout=timeout)
    if resp.status_code in (404, 204):
        return None
    resp.raise_for_status()
    rec_data = resp.json()

    if isinstance(rec_data, dict):
        raw_recs = rec_data.get("recommendations", [])
        if not isinstance(raw_recs, list):
            raw_recs = []
    elif isinstance(rec_data, list):
        raw_recs = rec_data
    else:
        raw_recs = []
    return raw_recs


def get_customer_recommendations(user_id, force_refresh=False):
    """
    Connect to Linux recommendation API:
    1. Read user preferences from SQLite user table (favTheme, favColor).
    2. Check if recommendations for current data are already available via GET.
       If available and force_refresh is False, reuse them without retraining!
    3. If not available or force_refresh is True:
       - Acquire concurrency lock (ensures refresh is called only once, preventing duplicate Spark/Maven runs).
       - Re-check if another request just finished refreshing while waiting for the lock.
       - Extract products, users, ratings from SQLite.
       - Send POST /api/recommendations/refresh with timeout=300s.
       - Fetch recommendations via GET /api/recommendations/<user_id> with timeout=15s.
    4. Enrich recommendations with actual SQLite product details and ratings.
    """
    recommendations = []
    conn = db()
    fav_theme = ""
    fav_color = ""
    has_preferences = False
    error_message = None

    try:
        # Check user preferences
        user_row = conn.execute(
            "SELECT favTheme, favColor FROM user WHERE idCli=?", (user_id,)
        ).fetchone()
        fav_theme = (user_row["favTheme"] or "").strip() if user_row else ""
        fav_color = (user_row["favColor"] or "").strip() if user_row else ""
        has_preferences = bool(fav_theme and fav_color)

        if not has_preferences:
            return {
                "recommendations": [],
                "total_recommendations": 0,
                "fav_theme": fav_theme,
                "fav_color": fav_color,
                "has_preferences": False,
                "has_custom_preferences": False,
                "error_message": None
            }

        base_url = os.getenv("RECOMMENDATION_API_BASE_URL", RECOMMENDATION_API_BASE_URL).rstrip("/")
        raw_recs = None

        # Step 1: If not explicitly forcing a refresh, check if recommendations already exist
        if not force_refresh:
            try:
                raw_recs = fetch_raw_recommendations(base_url, user_id, timeout=RECOMMENDATION_GET_TIMEOUT)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                # Network issue on simple GET; will be handled in outer try/catch below if refresh attempted
                pass
            except Exception as e:
                print(f"[recommendations] Existing check notice: {e}")

        # Step 2: If no recommendations available yet or refresh requested, trigger external training
        if force_refresh or not raw_recs:
            acquired = _refresh_lock.acquire(timeout=310)
            if not acquired:
                error_message = (
                    "Recommendation model training is currently in progress by another request. "
                    "Please wait a moment and try again."
                )
            else:
                try:
                    # Double-check: another thread might have finished the refresh while we waited for the lock
                    if not force_refresh:
                        try:
                            cached_recs = fetch_raw_recommendations(base_url, user_id, timeout=RECOMMENDATION_GET_TIMEOUT)
                            if cached_recs:
                                raw_recs = cached_recs
                        except Exception:
                            pass

                    # Only run the expensive refresh (Spark ALS, Maven, embeddings) if still needed
                    if force_refresh or not raw_recs:
                        # 1. Read products from SQLite offers table
                        cur = conn.execute(
                            "SELECT idOffer, name, description, category, color, theme FROM offers ORDER BY idOffer"
                        )
                        products = []
                        for row in cur.fetchall():
                            cat = (row["category"] or "").strip().lower()
                            col = (row["color"] or "").strip().lower()
                            thm = (row["theme"] or "").strip().lower()
                            if not cat or not thm or not col:
                                cat, col, thm = determine_product_metadata(
                                    row["name"] or "",
                                    row["description"] or "",
                                    cat, col, thm
                                )
                            products.append({
                                "productId": row["idOffer"],
                                "name":      row["name"] or "",
                                "category":  cat,
                                "color":     col,
                                "theme":     thm,
                            })

                        # 2. Read users from SQLite user table
                        cur = conn.execute(
                            "SELECT idCli, favTheme, favColor FROM user ORDER BY idCli"
                        )
                        users = []
                        for row in cur.fetchall():
                            users.append({
                                "userId":   row["idCli"],
                                "favTheme": row["favTheme"] or "",
                                "favColor": row["favColor"] or "",
                            })

                        # 3. Read ratings from SQLite ratings table
                        cur = conn.execute(
                            "SELECT idCli, idOffer, rating FROM ratings ORDER BY idCli, idOffer"
                        )
                        ratings = []
                        for row in cur.fetchall():
                            ratings.append({
                                "userId":    row["idCli"],
                                "productId": row["idOffer"],
                                "rating":    row["rating"],
                            })

                        # 4. Refresh external recommendation engine on Linux Master (timeout: 300 seconds)
                        refresh_url = f"{base_url}/api/recommendations/refresh"
                        payload = {
                            "products": products,
                            "users":    users,
                            "ratings":  ratings
                        }
                        refresh_resp = requests.post(
                            refresh_url, json=payload, timeout=RECOMMENDATION_REFRESH_TIMEOUT
                        )
                        refresh_resp.raise_for_status()

                        # 5. Fetch newly trained recommendations
                        raw_recs = fetch_raw_recommendations(base_url, user_id, timeout=RECOMMENDATION_GET_TIMEOUT)
                finally:
                    _refresh_lock.release()

        # Step 3: Enrich returned recommendations with actual SQLite product details
        if raw_recs:
            for idx, item in enumerate(raw_recs):
                prod_id = item.get("productId")
                if prod_id is None:
                    prod_id = item.get("idOffer") or item.get("product_id") or item.get("id")
                try:
                    prod_id = int(prod_id)
                except (ValueError, TypeError):
                    continue

                rank_val = item.get("rank")
                if rank_val is None or rank_val == "":
                    rank_val = idx + 1

                final_score = item.get("finalScore")
                if final_score is None:
                    final_score = item.get("final_score") or item.get("score") or ""

                als_score = item.get("alsScore")
                if als_score is None:
                    als_score = item.get("als_score") or ""

                trend_score = item.get("trendScore")
                if trend_score is None:
                    trend_score = item.get("trend_score") or ""

                item_name = (item.get("name") or "").strip()

                offer_row = conn.execute(
                    "SELECT idOffer, name, Offer_price, image_Name, description, duration, color, theme FROM offers WHERE idOffer=?",
                    (prod_id,)
                ).fetchone()

                rating_row = conn.execute(
                    "SELECT AVG(rating), COUNT(*) FROM ratings WHERE idOffer=?",
                    (prod_id,)
                ).fetchone()
                avg_rating = round(rating_row[0], 1) if (rating_row and rating_row[0] is not None) else 5.0
                rating_count = rating_row[1] if rating_row else 0

                if offer_row:
                    rec_item = {
                        "idOffer": offer_row["idOffer"],
                        "name": offer_row["name"] or item_name,
                        "Offer_price": offer_row["Offer_price"],
                        "image_Name": offer_row["image_Name"] or "default-product-image.png",
                        "description": offer_row["description"],
                        "duration": offer_row["duration"],
                        "color": offer_row["color"],
                        "theme": offer_row["theme"],
                        "rank": rank_val,
                        "finalScore": final_score,
                        "alsScore": als_score,
                        "trendScore": trend_score,
                        "avg_rating": avg_rating,
                        "rating_count": rating_count,
                        "recommendation_reason": f"Rank #{rank_val} • Score: {final_score}"
                    }
                else:
                    rec_item = {
                        "idOffer": prod_id,
                        "name": item_name or f"Product #{prod_id}",
                        "Offer_price": "40.0",
                        "image_Name": "default-product-image.png",
                        "description": "Recommended digital service based on machine learning collaborative filtering and social trend scoring.",
                        "duration": 30,
                        "color": "",
                        "theme": "Digital Service",
                        "rank": rank_val,
                        "finalScore": final_score,
                        "alsScore": als_score,
                        "trendScore": trend_score,
                        "avg_rating": avg_rating,
                        "rating_count": rating_count,
                        "recommendation_reason": f"Rank #{rank_val} • Score: {final_score}"
                    }

                recommendations.append(rec_item)

    except requests.exceptions.ConnectionError:
        error_message = (
            f"Unable to connect to the recommendation API at {base_url}. "
            "Please verify that the Linux Master server is running and accessible."
        )
        print(f"[recommendations] Connection error: {error_message}")
    except requests.exceptions.Timeout:
        error_message = (
            f"The recommendation engine timed out after {RECOMMENDATION_REFRESH_TIMEOUT} seconds. "
            "The Linux server may be under heavy load while training Spark ALS or building Maven. Please try again."
        )
        print(f"[recommendations] Timeout error: {error_message}")
    except requests.exceptions.HTTPError as http_err:
        status = http_err.response.status_code if http_err.response is not None else "Unknown"
        error_message = f"Recommendation service returned an error (HTTP {status})."
        print(f"[recommendations] HTTP error: {http_err}")
    except requests.exceptions.RequestException as req_err:
        error_message = f"Failed to communicate with recommendation service: {req_err}"
        print(f"[recommendations] Request exception: {req_err}")
    except Exception as exc:
        error_message = f"Unexpected error while loading recommendations: {exc}"
        print(f"[recommendations] Unexpected error: {exc}")

    finally:
        conn.close()

    return {
        "recommendations": recommendations,
        "total_recommendations": len(recommendations),
        "fav_theme": fav_theme,
        "fav_color": fav_color,
        "has_preferences": has_preferences,
        "has_custom_preferences": has_preferences,
        "error_message": error_message
    }


@app.route("/recommendations", methods=["GET"])
@app.route("/Recommendations", methods=["GET"])
def recommendations_page():
    if not session.get('logged_in'):
        flash("Please log in to view your personalized recommendations.", "info")
        return redirect('/Login')

    user_id = session.get('idCli')
    if not user_id:
        flash("Please log in to view your personalized recommendations.", "info")
        return redirect('/Login')

    force_refresh = request.args.get("refresh", "").lower() in ("true", "1", "yes")
    data = get_customer_recommendations(user_id, force_refresh=force_refresh)

    return render_template(
        "Recommendations.html",
        recommendations=data["recommendations"],
        total_recommendations=data["total_recommendations"],
        fav_theme=data["fav_theme"],
        fav_color=data["fav_color"],
        has_preferences=data["has_preferences"],
        has_custom_preferences=data["has_preferences"],
        error_message=data.get("error_message")
    )


@app.route("/logout", methods=["POST", "GET"])
def logout():
    for key in ["logged_in", "FirstName", "LastName", "Email", "role",
                "idCli", "whatsapp", "pfpName", "idCart", "fullprice",
                "ListOrders", "OrderNumber", "NumbSignup", "Revenue",
                "NumbOrders", "favTheme", "favColor"]:
        session.pop(key, None)
    return redirect("/home")


@app.route("/DeleteAccount", methods=["POST"])
def DeleteAccount():
    conn = db()
    try:
        conn.execute("DELETE FROM user WHERE email=?", (session["Email"],))
        conn.commit()
    finally:
        conn.close()
    return redirect("/logout")


@app.route("/UploadPFP", methods=["POST", "GET"])
def UploadPFP():
    profile_pic = request.files.get("file")
    if not profile_pic or not allowed_file(profile_pic.filename):
        return redirect('/USER_Page')

    pic_filename = secure_filename(profile_pic.filename)
    ext = pic_filename.rsplit('.', 1)[1].lower()
    upload_dir = os.path.join('static', 'Images', 'Users_pfp')
    app.config['UPLOAD_FOLDER'] = upload_dir

    pic_name = f"{session['Email']}_{session['FirstName']}.{ext}"

    if session.get("pfpName") and session["pfpName"] != "user583abc_1649114257.png":
        old_path = os.path.join(upload_dir, session["pfpName"])
        if os.path.exists(old_path):
            try:
                os.remove(old_path)
            except Exception:
                pass

    session["pfpName"] = pic_name

    conn = db()
    try:
        conn.execute(
            "UPDATE user SET pfpName=? WHERE email=?", (pic_name, session["Email"])
        )
        conn.commit()
    finally:
        conn.close()

    profile_pic.save(os.path.join(upload_dir, pic_name))
    return redirect("/home")


# ═══════════════════════════════════════════════════════════════════════════════
# ACTIVITY PAGE  (client / employee / admin)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/Activity_Page", methods=["POST", "GET"])
def Activity_Page():
    if not session.get('logged_in'):
        return redirect('/Login')

    role = session.get("role")
    conn = db()
    try:
        if role == "client":
            order_count = conn.execute(
                "SELECT COUNT(*) FROM orders "
                "WHERE StatOfTreatment='Pending Treatment' AND PaymentStat='Pending Payment' "
                "AND idCli=?",
                (session['idCli'],)
            ).fetchone()[0]

            orders_rows = conn.execute(
                "SELECT idOrder, StatOfTreatment, PaymentStat, TotalPrice FROM orders "
                "WHERE idCli=? AND StatOfTreatment='Pending Treatment' "
                "AND PaymentStat='Pending Payment'",
                (session["idCli"],)
            ).fetchall()

            list_orders = []
            for order in orders_rows:
                offers_names = conn.execute(
                    "SELECT o.name FROM orderoffers oo JOIN offers o ON oo.idOffer=o.idOffer "
                    "WHERE oo.idOrder=?", (order[0],)
                ).fetchall()
                list_orders.append({
                    'idOrder': order[0], 'StatOfTreatment': order[1],
                    'PaymentStat': order[2], 'TotalPrice': order[3],
                    'offers': [n[0] for n in offers_names]
                })

            active_sub_count = conn.execute(
                "SELECT COUNT(*) FROM subscription WHERE subscriptionStatus='Active' AND idCli=?",
                (session['idCli'],)
            ).fetchone()[0]

            subs_rows = conn.execute(
                "SELECT s.idSubscription, s.idOffer, s.subscriptionStatus, "
                "s.startDate, s.endDate, o.name "
                "FROM subscription s JOIN offers o ON s.idOffer=o.idOffer "
                "WHERE s.idCli=? ORDER BY s.subscriptionStatus ASC",
                (session['idCli'],)
            ).fetchall()

            current_date = datetime.now().date()
            for sub in subs_rows:
                if sub[4]:
                    try:
                        end = datetime.fromisoformat(str(sub[4])).date()
                        if end <= current_date:
                            conn.execute(
                                "UPDATE subscription SET subscriptionStatus='Inactive' "
                                "WHERE idSubscription=?", (sub[0],)
                            )
                            conn.commit()
                    except Exception:
                        pass

            list_subs = [{
                'idSubscription': s[0], 'idOffer': s[1],
                'subscriptionStatus': s[2], 'StartDate': s[3],
                'EndDate': s[4], 'OfferName': s[5]
            } for s in subs_rows]

            conn.close()
            return render_template(
                "Activity_Page.html", USR="client",
                ListOrders=list_orders, OrderNumber=order_count,
                ActiveSubNumber=active_sub_count, ListOfSubs=list_subs
            )

        else:  # employee or admin
            order_count = conn.execute(
                "SELECT COUNT(*) FROM orders "
                "WHERE StatOfTreatment='Pending Treatment' AND PaymentStat='Pending Payment'"
            ).fetchone()[0]

            orders_rows = conn.execute(
                "SELECT o.idOrder, o.StatOfTreatment, o.PaymentStat, o.TotalPrice, "
                "o.idCli, u.FirstName, u.LastName, u.Email, u.whatsapp "
                "FROM orders o JOIN user u ON o.idCli=u.idCli "
                "WHERE o.StatOfTreatment='Pending Treatment' AND o.PaymentStat='Pending Payment'"
            ).fetchall()

            list_orders = []
            for order in orders_rows:
                offers_names = conn.execute(
                    "SELECT o2.name FROM orderoffers oo JOIN offers o2 ON oo.idOffer=o2.idOffer "
                    "WHERE oo.idOrder=?", (order[0],)
                ).fetchall()
                list_orders.append({
                    'idOrder': order[0], 'StatOfTreatment': order[1],
                    'PaymentStat': order[2], 'TotalPrice': order[3],
                    'idCli': order[4], 'FirstName': order[5],
                    'LastName': order[6], 'Email': order[7],
                    'WhatsApp': order[8] or "",
                    'offers': [n[0] for n in offers_names]
                })

            numb_signup = conn.execute(
                "SELECT COUNT(*) FROM user WHERE role='client'"
            ).fetchone()[0]

            rev_row = conn.execute(
                "SELECT SUM(TotalPrice), COUNT(*) FROM orders WHERE PaymentStat='Payed'"
            ).fetchone()
            revenue     = rev_row[0] or 0
            numb_orders = rev_row[1]

            session['ListOrders']  = list_orders
            session['OrderNumber'] = order_count
            session['NumbSignup']  = numb_signup
            session['Revenue']     = revenue
            session['NumbOrders']  = numb_orders

            conn.close()
            if role == "admin":
                return render_template(
                    "Activity_Page.html", USR="admin",
                    ListOrders=list_orders, OrderNumber=order_count,
                    offerToModify=None, NumbSignup=numb_signup,
                    Revenue=revenue, NumbOrders=numb_orders
                )
            else:
                return render_template(
                    "Activity_Page.html", USR="employee",
                    ListOrders=list_orders, OrderNumber=order_count
                )
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise


# ─── Payment & orders ─────────────────────────────────────────────────────────
@app.route("/Payed", methods=["POST"])
def Payed():
    idOrder = request.form.get('idOrder')
    idCli   = request.form.get('idCli')
    conn = db()
    try:
        offers = conn.execute(
            "SELECT oo.idOffer, o.duration FROM offers o "
            "JOIN orderoffers oo ON oo.idOffer=o.idOffer WHERE oo.idOrder=?",
            (idOrder,)
        ).fetchall()

        current_date = datetime.now()
        for offer in offers:
            duration = timedelta(days=offer[1] or 0)
            end_date = (current_date + duration).isoformat()
            conn.execute(
                "INSERT INTO subscription (idCli, idOffer, subscriptionStatus, startDate, endDate) "
                "VALUES (?,?,?,?,?)",
                (idCli, offer[0], 'Active', current_date.isoformat(), end_date)
            )
        conn.commit()

        conn.execute(
            "UPDATE orders SET PaymentStat='Payed', StatOfTreatment='treated' WHERE idOrder=?",
            (idOrder,)
        )
        conn.commit()
    finally:
        conn.close()
    return redirect('Activity_Page')


@app.route("/Treated", methods=["POST"])
def Treated():
    idOrder = request.form.get('idOrder')
    conn = db()
    try:
        conn.execute(
            "UPDATE orders SET StatOfTreatment='treated' WHERE idOrder=?", (idOrder,)
        )
        conn.commit()
    finally:
        conn.close()
    return redirect('Activity_Page')


# ─── Admin user management ────────────────────────────────────────────────────
@app.route('/ChangeRole', methods=["POST"])
def ChangeRole():
    email_or_id = request.form.get('ChangeRoleInput', '')
    role        = request.form.get('Role')
    conn = db()
    try:
        if email_or_id.isdigit():
            conn.execute("UPDATE user SET role=? WHERE idCli=?", (role, email_or_id))
        else:
            conn.execute("UPDATE user SET role=? WHERE email=?", (role, email_or_id.lower()))
        conn.commit()
    finally:
        conn.close()
    return redirect('Activity_Page')


@app.route('/CheckOfferName', methods=["POST"])
def CheckOfferName():
    if request.method != "POST":
        return jsonify({"error": "Invalid request method"})
    name = request.json.get('name', '').lower()
    conn = db()
    try:
        row = conn.execute(
            "SELECT LOWER(name) FROM offers WHERE LOWER(name)=?", (name,)
        ).fetchone()
    finally:
        conn.close()
    return jsonify({"name_exist": "true" if row else "false"})


@app.route('/CheckOfferID', methods=["POST"])
def CheckOfferID():
    if request.method != "POST":
        return jsonify({"error": "Invalid request method"})
    offerID = request.json.get('offerID')
    conn = db()
    try:
        row = conn.execute(
            "SELECT idOffer FROM offers WHERE idOffer=?", (offerID,)
        ).fetchone()
    finally:
        conn.close()
    return jsonify({"offerID_exist": "true" if row else "false"})


@app.route("/AddOffer", methods=["POST"])
def AddOffer():
    offer_pic   = request.files.get("file")
    description = request.form.get('description')
    duration    = request.form.get('duration')
    price       = request.form.get('Offer_price')
    name        = request.form.get('name')
    color       = request.form.get('color', '').strip().lower()
    theme       = request.form.get('theme', '').strip().lower()
    category    = request.form.get('category', '').strip().lower()
    if not category and theme:
        category = theme
    elif not theme and category:
        theme = category
    category, color, theme = determine_product_metadata(name, description, category, color, theme)

    conn = db()
    try:
        cur = conn.execute(
            "INSERT INTO offers (description, duration, Offer_price, name, category, color, theme) "
            "VALUES (?,?,?,?,?,?,?)",
            (description, duration, price, name, category, color, theme)
        )
        conn.commit()
        offerID = cur.lastrowid

        if offer_pic and offer_pic.filename and allowed_file(offer_pic.filename):
            pic_filename = secure_filename(offer_pic.filename)
            ext = pic_filename.rsplit('.', 1)[1].lower()
            upload_dir = os.path.join('static', 'Images', 'product_pics')
            pic_name   = f"{name}.{ext}"
            offer_pic.save(os.path.join(upload_dir, pic_name))
            conn.execute(
                "UPDATE offers SET image_Name=? WHERE idOffer=?", (pic_name, offerID)
            )
            conn.commit()
    finally:
        conn.close()

    generate_products_csv()
    return redirect('Activity_Page')


@app.route("/RemoveOffer", methods=["POST"])
def RemoveOffer():
    offerID = request.form.get('offerID')
    conn = db()
    try:
        # Get image name before deletion
        row = conn.execute(
            "SELECT image_Name FROM offers WHERE idOffer=?", (offerID,)
        ).fetchone()

        for table in ["cartoffers", "orderoffers", "subscription", "ratings", "comment"]:
            col = "idOffer" if table != "cartoffers" else "idOffer"
            # cartoffers references idCart, not idOffer directly in the same way
            if table == "cartoffers":
                conn.execute("DELETE FROM cartoffers WHERE idOffer=?", (offerID,))
            else:
                conn.execute(f"DELETE FROM {table} WHERE idOffer=?", (offerID,))
        conn.commit()

        conn.execute("DELETE FROM offers WHERE idOffer=?", (offerID,))
        conn.commit()

        if row and row[0] and row[0] != 'default-product-image.png':
            path = os.path.join('static', 'Images', 'product_pics', row[0])
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass
    finally:
        conn.close()
    generate_products_csv()
    return redirect("/Activity_Page")


@app.route("/get_OfferToModify_Data", methods=["POST"])
def get_OfferToModify_Data():
    offerID = request.form.get('offerID')
    conn = db()
    try:
        row = conn.execute(
            "SELECT name, Offer_price, duration, description, color, theme, category "
            "FROM offers WHERE idOffer=?", (offerID,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return redirect('/Activity_Page')
    offerToModify = {
        'name': row[0], 'Offer_price': row[1], 'duration': row[2],
        'description': row[3], 'color': row[4] or '', 'theme': row[5] or '',
        'category': (row[6] or row[5] or ''),
        'offerID': offerID
    }
    return render_template("Activity_Page.html", offerToModify=offerToModify,
                           available_themes=AVAILABLE_THEMES, available_colors=AVAILABLE_COLORS)


@app.route("/modifyOffer", methods=["POST"])
def modifyOffer():
    offer_pic   = request.files.get("file")
    description = request.form.get('description')
    duration    = request.form.get('duration')
    price       = request.form.get('Offer_price')
    name        = request.form.get('name')
    offerID     = request.form.get('offerID')
    color       = request.form.get('color', '').strip().lower()
    theme       = request.form.get('theme', '').strip().lower()
    category    = request.form.get('category', '').strip().lower()
    if not category and theme:
        category = theme
    elif not theme and category:
        theme = category
    category, color, theme = determine_product_metadata(name, description, category, color, theme)

    conn = db()
    try:
        conn.execute(
            "UPDATE offers SET description=?, duration=?, Offer_price=?, name=?, category=?, color=?, theme=? "
            "WHERE idOffer=?",
            (description, duration, price, name, category, color, theme, offerID)
        )
        conn.commit()

        if offer_pic and offer_pic.filename and allowed_file(offer_pic.filename):
            pic_filename = secure_filename(offer_pic.filename)
            ext = pic_filename.rsplit('.', 1)[1].lower()
            upload_dir = os.path.join('static', 'Images', 'product_pics')
            pic_name   = f"{name}.{ext}"
            path = os.path.join(upload_dir, pic_name)
            if os.path.exists(path):
                os.remove(path)
            offer_pic.save(path)
            conn.execute(
                "UPDATE offers SET image_Name=? WHERE idOffer=?", (pic_name, offerID)
            )
            conn.commit()
    finally:
        conn.close()
    generate_products_csv()
    return redirect("/Activity_Page")


@app.route("/DeleteSub", methods=["POST", "GET"])
def DeleteSub():
    idSub = (request.form.get('idSub') if request.method == "POST"
             else request.args.get('idSub'))
    conn = db()
    try:
        conn.execute("DELETE FROM subscription WHERE idSubscription=?", (idSub,))
        conn.commit()
    finally:
        conn.close()
    return redirect("/Activity_Page")


@app.route("/RenewSub", methods=["POST"])
def RenewSub():
    idCli  = session['idCli']
    idOffer = request.form.get('idOffer')
    idSub   = request.form.get('idSub')

    conn = db()
    try:
        price_row = conn.execute(
            "SELECT Offer_price FROM offers WHERE idOffer=?", (idOffer,)
        ).fetchone()
        if not price_row:
            conn.close()
            return redirect('/Activity_Page')
        offer_price = price_row[0]

        cur = conn.execute(
            "INSERT INTO orders (idCli, TotalPrice) VALUES (?,?)", (idCli, offer_price)
        )
        conn.commit()
        idOrder = cur.lastrowid

        conn.execute(
            "INSERT INTO orderoffers (idOrder, idOffer) VALUES (?,?)", (idOrder, idOffer)
        )
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for('DeleteSub', idSub=idSub))


@app.route('/CheckUserID', methods=["POST"])
def CheckUserID():
    if request.method != "POST":
        return jsonify({"error": "Invalid request method"})
    user_id = request.json.get('UserID', '')
    conn = db()
    try:
        if user_id.isdigit():
            row = conn.execute(
                "SELECT idCli, role FROM user WHERE idCli=?", (user_id,)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT idCli, role FROM user WHERE email=?", (user_id.lower(),)
            ).fetchone()
    finally:
        conn.close()

    if not row:
        return jsonify({"UserID_exist": "false"})
    if row[1] == 'admin':
        return jsonify({"UserID_exist": "admin"})
    return jsonify({"UserID_exist": "true"})


@app.route("/RemoveUSER", methods=["POST"])
def RemoveUSER():
    user_id = request.form.get('UserID')
    conn = db()
    try:
        conn.execute(
            "DELETE FROM cartoffers WHERE idCart IN (SELECT idCart FROM cart WHERE idCli=?)",
            (user_id,)
        )
        conn.execute("DELETE FROM cart WHERE idCli=?", (user_id,))
        conn.execute(
            "DELETE FROM orderoffers WHERE idOrder IN (SELECT idOrder FROM orders WHERE idCli=?)",
            (user_id,)
        )
        conn.execute("UPDATE orders SET idCli=NULL WHERE idCli=?", (user_id,))
        conn.execute("DELETE FROM subscription WHERE idCli=?", (user_id,))
        conn.execute("DELETE FROM ratings WHERE idCli=?", (user_id,))
        conn.execute("DELETE FROM comment WHERE idCli=?", (user_id,))
        conn.execute("DELETE FROM user WHERE idCli=?", (user_id,))
        conn.commit()
    finally:
        conn.close()
    return redirect("/Activity_Page")


@app.route("/showUsers", methods=["POST"])
def showUsers():
    conn = db()
    try:
        def fetch_role(r):
            rows = conn.execute(
                "SELECT idCli, Email, FirstName, LastName, status, role, Date_Joined, whatsapp "
                f"FROM user WHERE role=?", (r,)
            ).fetchall()
            return [{
                'idCli': x[0], 'Email': x[1], 'FirstName': x[2], 'LastName': x[3],
                'status': x[4], 'role': x[5], 'Date_Joined': x[6], 'whatsapp': x[7] or ""
            } for x in rows]

        clients   = fetch_role('client')
        employees = fetch_role('employee')
        admins    = fetch_role('admin')
    finally:
        conn.close()

    return render_template(
        "Activity_Page.html", USR="admin",
        ListOrders=session.get('ListOrders', []),
        OrderNumber=session.get('OrderNumber', 0),
        offerToModify=None,
        NumbSignup=session.get('NumbSignup', 0),
        Revenue=session.get('Revenue', 0),
        NumbOrders=session.get('NumbOrders', 0),
        ListOfClients=clients,
        ListOfEmployees=employees,
        ListOfAdmins=admins
    )



# ═══════════════════════════════════════════════════════════════════════════════
# PRODUCER STUDIO, RATINGS & TRENDS
# ═══════════════════════════════════════════════════════════════════════════════

def _require_producer_or_admin():
    if not session.get('logged_in'):
        return redirect('/Login')
    if session.get('role') not in ('producer', 'admin'):
        flash("Access restricted to digital service producers and administrators.", "danger")
        return redirect('/home')
    return None


@app.route("/producer/dashboard", methods=["GET"])
def producer_dashboard():
    guard = _require_producer_or_admin()
    if guard:
        return guard

    user_id = session.get("idCli")
    is_admin = session.get("role") == "admin"

    conn = db()
    try:
        if is_admin:
            query = """
                SELECT o.idOffer, o.name, o.description, o.Offer_price, o.duration,
                       o.image_Name, o.color, o.theme, o.producer_id,
                       u.FirstName || ' ' || u.LastName as producer_name,
                       AVG(r.rating) as avg_rating,
                       COUNT(r.idRating) as rating_count
                FROM offers o
                LEFT JOIN user u ON o.producer_id = u.idCli
                LEFT JOIN ratings r ON o.idOffer = r.idOffer
                GROUP BY o.idOffer
                ORDER BY o.idOffer DESC
            """
            products = conn.execute(query).fetchall()
            comm_cnt = conn.execute("SELECT COUNT(*) FROM comment").fetchone()[0]
        else:
            query = """
                SELECT o.idOffer, o.name, o.description, o.Offer_price, o.duration,
                       o.image_Name, o.color, o.theme, o.producer_id,
                       AVG(r.rating) as avg_rating,
                       COUNT(r.idRating) as rating_count
                FROM offers o
                LEFT JOIN ratings r ON o.idOffer = r.idOffer
                WHERE o.producer_id = ?
                GROUP BY o.idOffer
                ORDER BY o.idOffer DESC
            """
            products = conn.execute(query, (user_id,)).fetchall()
            comm_cnt = conn.execute(
                "SELECT COUNT(*) FROM comment c JOIN offers o ON c.idOffer = o.idOffer WHERE o.producer_id=?",
                (user_id,)
            ).fetchone()[0]

        total_products = len(products)
        total_ratings = sum(p["rating_count"] or 0 for p in products)
        rated_prods = [p["avg_rating"] for p in products if p["avg_rating"]]
        overall_avg = round(sum(rated_prods) / len(rated_prods), 1) if rated_prods else 0.0
    finally:
        conn.close()

    insights = {
        "total_products": total_products,
        "total_ratings": total_ratings,
        "overall_avg": overall_avg,
        "total_comments": comm_cnt
    }

    return render_template(
        "Producer_Dashboard.html",
        products=products,
        insights=insights,
        themes=AVAILABLE_THEMES,
        colors=AVAILABLE_COLORS
    )


@app.route("/producer/product/add", methods=["POST"])
def producer_add_product():
    guard = _require_producer_or_admin()
    if guard:
        return guard

    name        = request.form.get("name", "").strip()
    description = request.form.get("description", "").strip()
    price_val   = request.form.get("Offer_price") or request.form.get("price") or 0
    price       = float(price_val)
    duration    = int(request.form.get("duration", 30))
    color       = request.form.get("color", "").strip().lower()
    theme       = request.form.get("theme", "").strip().lower()
    category    = request.form.get("category", "").strip().lower()
    if not category and theme:
        category = theme
    elif not theme and category:
        theme = category
    category, color, theme = determine_product_metadata(name, description, category, color, theme)
    producer_id = session.get("idCli")
    offer_pic   = request.files.get("file")

    conn = db()
    try:
        cur = conn.execute(
            "INSERT INTO offers (name, description, Offer_price, duration, category, color, theme, producer_id, image_Name) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (name, description, price, duration, category, color, theme, producer_id, "default-product-image.png")
        )
        conn.commit()
        offer_id = cur.lastrowid

        if offer_pic and offer_pic.filename and allowed_file(offer_pic.filename):
            pic_filename = secure_filename(offer_pic.filename)
            ext = pic_filename.rsplit('.', 1)[1].lower()
            upload_dir = os.path.join('static', 'Images', 'product_pics')
            os.makedirs(upload_dir, exist_ok=True)
            pic_name = f"{name}_{offer_id}.{ext}"
            offer_pic.save(os.path.join(upload_dir, pic_name))
            conn.execute("UPDATE offers SET image_Name=? WHERE idOffer=?", (pic_name, offer_id))
            conn.commit()
    finally:
        conn.close()

    generate_products_csv()
    return redirect("/producer/dashboard")


@app.route("/producer/product/edit/<int:product_id>", methods=["POST"])
def producer_edit_product(product_id):
    guard = _require_producer_or_admin()
    if guard:
        return guard

    user_id = session.get("idCli")
    is_admin = session.get("role") == "admin"

    conn = db()
    try:
        offer = conn.execute("SELECT idOffer, producer_id, image_Name FROM offers WHERE idOffer=?", (product_id,)).fetchone()
        if not offer:
            return "Product not found", 404
        if not is_admin and offer["producer_id"] != user_id:
            return "Unauthorized: You can only edit your own products", 403

        name        = request.form.get("name", "").strip()
        description = request.form.get("description", "").strip()
        price_val   = request.form.get("Offer_price") or request.form.get("price") or 0
        price       = float(price_val)
        duration    = int(request.form.get("duration", 30))
        color       = request.form.get("color", "").strip().lower()
        theme       = request.form.get("theme", "").strip().lower()
        category    = request.form.get("category", "").strip().lower()
        if not category and theme:
            category = theme
        elif not theme and category:
            theme = category
        category, color, theme = determine_product_metadata(name, description, category, color, theme)
        offer_pic   = request.files.get("file")

        conn.execute(
            "UPDATE offers SET name=?, description=?, Offer_price=?, duration=?, category=?, color=?, theme=? WHERE idOffer=?",
            (name, description, price, duration, category, color, theme, product_id)
        )
        conn.commit()

        if offer_pic and offer_pic.filename and allowed_file(offer_pic.filename):
            pic_filename = secure_filename(offer_pic.filename)
            ext = pic_filename.rsplit('.', 1)[1].lower()
            upload_dir = os.path.join('static', 'Images', 'product_pics')
            os.makedirs(upload_dir, exist_ok=True)
            pic_name = f"{name}_{product_id}.{ext}"
            offer_pic.save(os.path.join(upload_dir, pic_name))
            conn.execute("UPDATE offers SET image_Name=? WHERE idOffer=?", (pic_name, product_id))
            conn.commit()
    finally:
        conn.close()

    generate_products_csv()
    return redirect("/producer/dashboard")


@app.route("/producer/product/delete/<int:product_id>", methods=["POST"])
def producer_delete_product(product_id):
    guard = _require_producer_or_admin()
    if guard:
        return guard

    user_id = session.get("idCli")
    is_admin = session.get("role") == "admin"

    conn = db()
    try:
        offer = conn.execute("SELECT idOffer, producer_id FROM offers WHERE idOffer=?", (product_id,)).fetchone()
        if not offer:
            return "Product not found", 404
        if not is_admin and offer["producer_id"] != user_id:
            return "Unauthorized: You can only delete your own products", 403

        with conn:
            conn.execute("DELETE FROM cartoffers WHERE idOffer=?", (product_id,))
            conn.execute("DELETE FROM orderoffers WHERE idOffer=?", (product_id,))
            conn.execute("DELETE FROM ratings WHERE idOffer=?", (product_id,))
            conn.execute("DELETE FROM comment WHERE idOffer=?", (product_id,))
            conn.execute("DELETE FROM subscription WHERE idOffer=?", (product_id,))
            conn.execute("DELETE FROM offers WHERE idOffer=?", (product_id,))
    finally:
        conn.close()

    generate_products_csv()
    generate_ratings_csv()
    return redirect("/producer/dashboard")


@app.route("/producer/ratings", methods=["GET"])
def producer_ratings():
    guard = _require_producer_or_admin()
    if guard:
        return guard

    user_id = session.get("idCli")
    is_admin = session.get("role") == "admin"

    conn = db()
    try:
        if is_admin:
            my_offers = conn.execute(
                "SELECT idOffer, name, Offer_price, image_Name, theme FROM offers ORDER BY idOffer"
            ).fetchall()
        else:
            my_offers = conn.execute(
                "SELECT idOffer, name, Offer_price, image_Name, theme FROM offers WHERE producer_id=? ORDER BY idOffer",
                (user_id,)
            ).fetchall()

        ratings_data = []
        for o in my_offers:
            oid = o["idOffer"]
            summary = conn.execute(
                "SELECT AVG(rating), COUNT(idRating) FROM ratings WHERE idOffer=?", (oid,)
            ).fetchone()
            avg_r = round(summary[0], 1) if summary[0] else 0.0
            total_r = summary[1] or 0

            distribution = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
            dist_rows = conn.execute(
                "SELECT rating, COUNT(*) FROM ratings WHERE idOffer=? GROUP BY rating", (oid,)
            ).fetchall()
            for r_val, count in dist_rows:
                distribution[r_val] = count

            dist_pct = {
                star: round((distribution[star] / total_r * 100), 1) if total_r > 0 else 0
                for star in range(1, 6)
            }

            comments = conn.execute(
                "SELECT c.comment_TEXT, c.Date_written, u.FirstName, u.LastName, u.Email, u.pfpName "
                "FROM comment c JOIN user u ON c.idCli = u.idCli "
                "WHERE c.idOffer=? ORDER BY c.idComment DESC",
                (oid,)
            ).fetchall()

            ratings_data.append({
                "offer": o,
                "avg_rating": avg_r,
                "total_ratings": total_r,
                "distribution": distribution,
                "dist_pct": dist_pct,
                "comments": comments
            })
    finally:
        conn.close()

    return render_template("Producer_Ratings.html", ratings_data=ratings_data)


# ═══════════════════════════════════════════════════════════════════════════════
# TREND ANALYSIS (Linux Master API Integration + CSV Datasets)
# ═══════════════════════════════════════════════════════════════════════════════

LINUX_API_BASE_URL = os.getenv("LINUX_API_BASE_URL", "http://<LINUX_VM_HOST>:5001")

# In-memory storage for latest hourly trends results (since no hourly CSV exists)
LATEST_HOURLY_DATA = {
    "trends": [],
    "last_updated": None,
    "date": "",
    "hour": ""
}


def get_trends_csv_path(filename):
    """Resolve path to trend CSV file, checking csv_exports/ first, then workspace root."""
    primary = os.path.join(CSV_OUTPUT_DIR, filename)
    if os.path.isfile(primary):
        return primary
    fallback = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    if os.path.isfile(fallback):
        return fallback
    return primary


def load_trends_file(filename):
    """
    Load trends from CSV file with two columns: topic,frequency.
    Returns (trends_list, last_updated_str).
    Sorted descending by frequency with 1-based ranks.
    """
    filepath = get_trends_csv_path(filename)
    trends = []
    last_updated = None

    if os.path.isfile(filepath):
        try:
            mtime = os.path.getmtime(filepath)
            last_updated = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
            with open(filepath, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    topic = (row.get("topic") or "").strip()
                    if not topic:
                        continue
                    try:
                        freq = int(float(row.get("frequency", 0)))
                    except (ValueError, TypeError):
                        freq = 0
                    trends.append({
                        "topic": topic,
                        "frequency": freq
                    })
        except Exception as e:
            print(f"[trends] Error loading {filename}: {e}")

    trends.sort(key=lambda x: x["frequency"], reverse=True)
    for rank, item in enumerate(trends, start=1):
        item["rank"] = rank

    return trends, last_updated


def load_global_trends():
    """Load global trends from global_trends.csv (topic,frequency)."""
    return load_trends_file("global_trends.csv")


def load_daily_trends():
    """Load daily trends from daily_trends.csv (topic,frequency)."""
    return load_trends_file("daily_trends.csv")


def load_hourly_trends():
    """Load hourly trends from hourly_trends.csv if it exists, otherwise from memory."""
    hourly_csv = get_trends_csv_path("hourly_trends.csv")
    if os.path.isfile(hourly_csv):
        return load_trends_file("hourly_trends.csv")
    return LATEST_HOURLY_DATA.get("trends", []), LATEST_HOURLY_DATA.get("last_updated")


def save_trends_to_csv(filepath, trends_list):
    """
    Write trends to CSV with exact header 'topic,frequency'.
    Atomically overwrites the file.
    """
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    temp_path = filepath + ".tmp"
    with open(temp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["topic", "frequency"])
        for item in trends_list:
            writer.writerow([item["topic"], item["frequency"]])
    if os.path.exists(filepath):
        os.replace(temp_path, filepath)
    else:
        os.rename(temp_path, filepath)


def parse_trends_json_response(data):
    """
    Extract list of dicts [{'topic': str, 'frequency': int}] from Linux API JSON response.
    Handles list of dicts, {'trends': [...]}, {'data': [...]}, or dict mappings.
    """
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for key in ("trends", "data", "results", "topics", "items"):
            if key in data and isinstance(data[key], list):
                items = data[key]
                break
        if not items:
            if all(isinstance(v, (int, float, str)) for v in data.values()) and not any(k in ("status", "message", "error", "code") for k in data.keys()):
                items = [{"topic": k, "frequency": v} for k, v in data.items()]
            elif "topic" in data:
                items = [data]

    trends = []
    for item in items:
        if isinstance(item, dict):
            topic = item.get("topic") or item.get("name") or item.get("keyword") or item.get("key") or ""
            freq = item.get("frequency")
            if freq is None:
                freq = item.get("count")
            if freq is None:
                freq = item.get("value")
            if freq is None:
                freq = item.get("total", 0)
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            topic = item[0]
            freq = item[1]
        else:
            continue

        topic = str(topic).strip()
        if not topic:
            continue
        try:
            freq = int(float(freq))
        except (ValueError, TypeError):
            freq = 0
        trends.append({"topic": topic, "frequency": freq})

    trends.sort(key=lambda x: x["frequency"], reverse=True)
    for rank, item in enumerate(trends, start=1):
        item["rank"] = rank

    return trends


def trigger_external_trend_refresh():
    """
    Separate, replaceable hook function for future API integration.
    Currently reloads the local CSV files directly from disk without calculations.
    """
    global_path = get_trends_csv_path("global_trends.csv")
    daily_path = get_trends_csv_path("daily_trends.csv")
    return {
        "status": "success",
        "message": "CSV datasets reloaded successfully.",
        "global_found": os.path.exists(global_path),
        "daily_found": os.path.exists(daily_path),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


@app.route("/trends", methods=["GET"])
def trends_page():
    """
    Display trend analysis from saved CSV files.
    Opening the page ONLY displays the latest saved results (no automatic processing).
    Supports Global Trends, Daily Trends, and Hourly Trends tabs.
    """
    tab = request.args.get("tab", "global").lower()
    if tab not in ("global", "daily", "hourly"):
        tab = "global"

    if tab == "global":
        trends, last_updated = load_global_trends()
    elif tab == "daily":
        trends, last_updated = load_daily_trends()
    else:
        trends, last_updated = load_hourly_trends()

    return render_template(
        "Trends.html",
        tab=tab,
        trends=trends,
        last_updated=last_updated,
        total_count=len(trends),
        hourly_meta={"date": LATEST_HOURLY_DATA.get("date"), "hour": LATEST_HOURLY_DATA.get("hour")} if tab == "hourly" else None,
        linux_api_url=LINUX_API_BASE_URL
    )


@app.route("/trends/refresh", methods=["POST"])
@app.route("/trends/refresh/<trend_type>", methods=["POST"])
def trends_refresh(trend_type=None):
    """
    Admin-only endpoint to trigger trend recalculation on Linux Master (<LINUX_VM_HOST>:5001).
    Endpoints called on Linux Master:
      - Global: POST /api/trends/global
      - Daily:  POST /api/trends/daily?date=YYYY-MM-DD
      - Hourly: POST /api/trends/hourly?date=YYYY-MM-DD&hour=HH
    Overwrites global_trends.csv or daily_trends.csv upon success.
    Ordinary users cannot trigger this endpoint (returns 403).
    """
    is_ajax = request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest"

    # 3. Admin-only authorization
    if not session.get('logged_in') or session.get('role') != 'admin':
        if is_ajax:
            return jsonify({"success": False, "error": "Administrator permission required."}), 403
        flash("Administrator permission required.", "error")
        return redirect("/trends"), 403

    # Determine trend type
    if not trend_type:
        if request.is_json:
            payload = request.get_json(silent=True) or {}
            trend_type = payload.get("trend_type") or payload.get("tab")
        else:
            trend_type = request.form.get("trend_type") or request.form.get("tab")
    trend_type = (trend_type or "global").lower().strip()

    if trend_type not in ("global", "daily", "hourly"):
        err = f"Invalid trend type: {trend_type}. Must be 'global', 'daily', or 'hourly'."
        if is_ajax:
            return jsonify({"success": False, "error": err}), 400
        flash(err, "error")
        return redirect("/trends"), 400

    # Retrieve date and hour parameters
    date_val = None
    hour_val = None
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        date_val = payload.get("date")
        hour_val = payload.get("hour")
    else:
        date_val = request.form.get("date") or request.args.get("date")
        hour_val = request.form.get("hour") or request.args.get("hour")

    date_str = str(date_val).strip() if date_val else ""
    hour_str = str(hour_val).strip() if hour_val is not None else ""

    # Validate inputs
    if trend_type in ("daily", "hourly"):
        if not date_str:
            err = "Date parameter (YYYY-MM-DD) is required."
            if is_ajax:
                return jsonify({"success": False, "error": err}), 400
            flash(err, "error")
            return redirect(f"/trends?tab={trend_type}"), 400
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            err = f"Invalid date format '{date_str}'. Expected format is YYYY-MM-DD (e.g., 2023-01-15)."
            if is_ajax:
                return jsonify({"success": False, "error": err}), 400
            flash(err, "error")
            return redirect(f"/trends?tab={trend_type}"), 400

    hour_param = None
    if trend_type == "hourly":
        if not hour_str:
            err = "Hour parameter (0–23) is required."
            if is_ajax:
                return jsonify({"success": False, "error": err}), 400
            flash(err, "error")
            return redirect("/trends?tab=hourly"), 400
        try:
            hour_int = int(hour_str)
            if not (0 <= hour_int <= 23):
                raise ValueError()
            hour_param = f"{hour_int:02d}"
        except ValueError:
            err = f"Invalid hour '{hour_str}'. Expected an integer between 0 and 23."
            if is_ajax:
                return jsonify({"success": False, "error": err}), 400
            flash(err, "error")
            return redirect("/trends?tab=hourly"), 400

    # 4. Connect to Linux API from Windows Flask backend
    # Global: POST /api/trends/global
    # Daily:  POST /api/trends/daily?date=YYYY-MM-DD
    # Hourly: POST /api/trends/hourly?date=YYYY-MM-DD&hour=HH
    try:
        if trend_type == "global":
            endpoint = f"{LINUX_API_BASE_URL}/api/trends/global"
            resp = requests.post(endpoint, timeout=45)
        elif trend_type == "daily":
            endpoint = f"{LINUX_API_BASE_URL}/api/trends/daily"
            resp = requests.post(endpoint, params={"date": date_str}, timeout=45)
        else:
            endpoint = f"{LINUX_API_BASE_URL}/api/trends/hourly"
            resp = requests.post(endpoint, params={"date": date_str, "hour": hour_param}, timeout=45)
    except requests.exceptions.Timeout:
        err = f"Connection timed out contacting Linux API at {LINUX_API_BASE_URL} (timeout 45s)."
        if is_ajax:
            return jsonify({"success": False, "error": err}), 504
        flash(err, "error")
        return redirect(f"/trends?tab={trend_type}"), 504
    except requests.exceptions.ConnectionError:
        err = f"Connection refused: unable to connect to Linux API at {LINUX_API_BASE_URL}."
        if is_ajax:
            return jsonify({"success": False, "error": err}), 502
        flash(err, "error")
        return redirect(f"/trends?tab={trend_type}"), 502
    except requests.exceptions.RequestException as e:
        err = f"Error communicating with Linux API: {str(e)}"
        if is_ajax:
            return jsonify({"success": False, "error": err}), 500
        flash(err, "error")
        return redirect(f"/trends?tab={trend_type}"), 500

    if resp.status_code != 200:
        err = f"Linux API returned error (HTTP {resp.status_code}): {resp.text[:300]}"
        if is_ajax:
            return jsonify({"success": False, "error": err}), resp.status_code
        flash(err, "error")
        return redirect(f"/trends?tab={trend_type}"), resp.status_code

    try:
        data = resp.json()
    except Exception as e:
        err = f"Failed to parse JSON response from Linux API: {str(e)}"
        if is_ajax:
            return jsonify({"success": False, "error": err}), 502
        flash(err, "error")
        return redirect(f"/trends?tab={trend_type}"), 502

    parsed_trends = parse_trends_json_response(data)

    # 5. Update existing CSV files
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if trend_type == "global":
        csv_path = os.path.join(CSV_OUTPUT_DIR, "global_trends.csv")
        save_trends_to_csv(csv_path, parsed_trends)
        success_msg = f"Global trends recalculated successfully ({len(parsed_trends)} topics loaded)."
    elif trend_type == "daily":
        csv_path = os.path.join(CSV_OUTPUT_DIR, "daily_trends.csv")
        save_trends_to_csv(csv_path, parsed_trends)
        success_msg = f"Daily trends for {date_str} recalculated successfully ({len(parsed_trends)} topics loaded)."
    else:
        hourly_csv_path = os.path.join(CSV_OUTPUT_DIR, "hourly_trends.csv")
        if os.path.isfile(hourly_csv_path):
            save_trends_to_csv(hourly_csv_path, parsed_trends)
        LATEST_HOURLY_DATA["trends"] = parsed_trends
        LATEST_HOURLY_DATA["last_updated"] = now_str
        LATEST_HOURLY_DATA["date"] = date_str
        LATEST_HOURLY_DATA["hour"] = hour_param
        success_msg = f"Hourly trends for {date_str} {hour_param}:00 recalculated successfully ({len(parsed_trends)} topics loaded)."

    if is_ajax:
        return jsonify({
            "success": True,
            "message": success_msg,
            "count": len(parsed_trends),
            "trends": parsed_trends,
            "tab": trend_type,
            "last_updated": now_str
        })

    flash(success_msg, "success")
    return redirect(f"/trends?tab={trend_type}")



# ═══════════════════════════════════════════════════════════════════════════════
# CSV EXPORT (admin only)
# ═══════════════════════════════════════════════════════════════════════════════

def _require_admin():
    """Return None if OK, or a redirect response if not admin."""
    if not session.get('logged_in') or session.get('role') != 'admin':
        return redirect('/Login')
    return None


@app.route("/admin/csv", methods=["GET"])
def admin_csv_page():
    guard = _require_admin()
    if guard:
        return guard
    return render_template("Admin_CSV.html", results=None,
                           output_dir=os.path.abspath(CSV_OUTPUT_DIR))


@app.route("/admin/csv/generate_all", methods=["POST"])
def admin_csv_generate_all():
    guard = _require_admin()
    if guard:
        return guard
    results = generate_all_csvs()
    return render_template("Admin_CSV.html", results=results,
                           output_dir=os.path.abspath(CSV_OUTPUT_DIR))


@app.route("/admin/csv/generate/<which>", methods=["POST"])
def admin_csv_generate_one(which):
    guard = _require_admin()
    if guard:
        return guard
    generators = {
        'products': generate_products_csv,
        'users':    generate_users_csv,
        'ratings':  generate_ratings_csv,
        'trends':   generate_trends_csv,
    }
    if which not in generators:
        return redirect('/admin/csv')
    result = generators[which]()
    results = {which: result, "output_dir": os.path.abspath(CSV_OUTPUT_DIR),
               "generated_at": datetime.now().isoformat(timespec="seconds")}
    return render_template("Admin_CSV.html", results=results,
                           output_dir=os.path.abspath(CSV_OUTPUT_DIR))


@app.route("/admin/csv/download/<filename>")
def admin_csv_download(filename):
    guard = _require_admin()
    if guard:
        return guard
    # Security: only allow the four known filenames
    allowed = {"products.csv", "users.csv", "ratings.csv", "trends_mock.csv"}
    if filename not in allowed:
        return "Not found", 404
    return send_from_directory(
        os.path.abspath(CSV_OUTPUT_DIR), filename, as_attachment=True
    )


@app.route("/admin/csv/import_trends", methods=["POST"])
def admin_import_trends():
    """
    Accept trend data from the Kafka/Spark pipeline.
    Expects JSON body: {"trends": [{"topic": "...", "frequency": 123}, ...]}
    Or form-submitted CSV text with lines: topic,frequency
    """
    guard = _require_admin()
    if guard:
        return guard

    data = request.get_json(silent=True)
    if data and "trends" in data:
        pairs = [(t['topic'], int(t['frequency'])) for t in data['trends']
                 if 'topic' in t and 'frequency' in t]
    else:
        raw = request.form.get('trends_csv', '')
        pairs = []
        for line in raw.strip().splitlines():
            parts = line.strip().split(',')
            if len(parts) == 2:
                try:
                    pairs.append((parts[0].strip(), int(parts[1].strip())))
                except ValueError:
                    pass

    if not pairs:
        return jsonify({"error": "No valid trend pairs found"}), 400

    result = import_real_trends(pairs)
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# JSON DATA IMPORT ENDPOINTS (admin only)
# ─────────────────────────────────────────────────────────────────────────────

def _get_request_json():
    """Extract and parse JSON from request, handling application/json or raw text."""
    if request.is_json:
        data = request.get_json(silent=True)
        if data is not None:
            return data, None
    raw = request.form.get('json_data') or request.get_data(as_text=True)
    if not raw or not raw.strip():
        return None, "Empty payload. Please submit a JSON array."
    try:
        return json.loads(raw), None
    except Exception as e:
        return None, f"Malformed JSON: {str(e)}"


@app.route("/admin/import/users", methods=["POST"])
@app.route("/admin/csv/import/users", methods=["POST"])
def admin_import_users():
    guard = _require_admin()
    if guard:
        return jsonify({"success": False, "error": "Administrator permission required."}), 403

    data, err = _get_request_json()
    if err:
        return jsonify({"success": False, "error": err}), 400

    if isinstance(data, dict) and "users" in data:
        data = data["users"]

    if not isinstance(data, list):
        return jsonify({"success": False, "error": "Expected a JSON array of user records."}), 400

    if len(data) == 0:
        return jsonify({"success": False, "error": "JSON array cannot be empty."}), 400

    validated = []
    seen_ids = set()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            return jsonify({"success": False, "error": f"Item at index {idx} must be a JSON object."}), 400

        user_id_raw = item.get("userId") if item.get("userId") is not None else item.get("idCli")
        if user_id_raw is None:
            return jsonify({"success": False, "error": f"Item at index {idx} is missing required 'userId'."}), 400

        try:
            user_id = int(user_id_raw)
            if user_id <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": f"Item at index {idx}: 'userId' must be a positive integer (got {user_id_raw})."}), 400

        if user_id in seen_ids:
            return jsonify({"success": False, "error": f"Duplicate userId {user_id} detected in import batch."}), 400
        seen_ids.add(user_id)

        fav_theme = str(item.get("favTheme") or item.get("theme") or "").strip().lower()
        fav_color = str(item.get("favColor") or item.get("color") or "").strip().lower()
        email = str(item.get("email") or item.get("Email") or f"testuser_{user_id}@damsostream.test").strip().lower()
        first_name = str(item.get("firstName") or item.get("FirstName") or f"User{user_id}").strip()
        last_name = str(item.get("lastName") or item.get("LastName") or "Test").strip()
        password = str(item.get("password") or item.get("Password") or "TestPass@123")
        status = str(item.get("status") or "verified")
        role = str(item.get("role") or "client")
        date_joined = str(item.get("date_joined") or item.get("Date_Joined") or now_str)
        whatsapp = str(item.get("whatsapp") or "")
        pfp_name = str(item.get("pfpName") or "user583abc_1649114257.png")

        validated.append({
            "idCli": user_id,
            "Email": email,
            "Password": password,
            "FirstName": first_name,
            "LastName": last_name,
            "status": status,
            "role": role,
            "Date_Joined": date_joined,
            "whatsapp": whatsapp,
            "pfpName": pfp_name,
            "favTheme": fav_theme,
            "favColor": fav_color
        })

    conn = db()
    try:
        # Pre-check database for conflicts to avoid partial state and deliver precise errors
        for u in validated:
            existing_user = conn.execute("SELECT idCli FROM user WHERE idCli=?", (u["idCli"],)).fetchone()
            if existing_user:
                return jsonify({"success": False, "error": f"User ID {u['idCli']} already exists in database. Overwriting is not permitted."}), 409

            existing_email = conn.execute("SELECT idCli FROM user WHERE LOWER(Email)=?", (u["Email"].lower(),)).fetchone()
            if existing_email:
                return jsonify({"success": False, "error": f"Email '{u['Email']}' is already used by User ID {existing_email['idCli']}."}), 409

        # Transactional insert
        with conn:
            for u in validated:
                conn.execute(
                    "INSERT INTO user (idCli, Email, Password, FirstName, LastName, status, role, Date_Joined, whatsapp, pfpName, favTheme, favColor) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (u["idCli"], u["Email"], u["Password"], u["FirstName"], u["LastName"], u["status"], u["role"], u["Date_Joined"], u["whatsapp"], u["pfpName"], u["favTheme"], u["favColor"])
                )
                conn.execute("INSERT OR IGNORE INTO cart (idCli) VALUES (?)", (u["idCli"],))
    except sqlite3.IntegrityError as e:
        return jsonify({"success": False, "error": f"Database integrity error: {str(e)}"}), 409
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to import users: {str(e)}"}), 500
    finally:
        conn.close()

    csv_res = generate_users_csv()
    return jsonify({
        "success": True,
        "count": len(validated),
        "message": f"Successfully imported {len(validated)} user(s). users.csv has been updated.",
        "csv_result": csv_res
    })


@app.route("/admin/import/products", methods=["POST"])
@app.route("/admin/csv/import/products", methods=["POST"])
def admin_import_products():
    guard = _require_admin()
    if guard:
        return jsonify({"success": False, "error": "Administrator permission required."}), 403

    data, err = _get_request_json()
    if err:
        return jsonify({"success": False, "error": err}), 400

    if isinstance(data, dict) and "products" in data:
        data = data["products"]

    if not isinstance(data, list):
        return jsonify({"success": False, "error": "Expected a JSON array of product records."}), 400

    if len(data) == 0:
        return jsonify({"success": False, "error": "JSON array cannot be empty."}), 400

    validated = []
    seen_ids = set()

    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            return jsonify({"success": False, "error": f"Item at index {idx} must be a JSON object."}), 400

        prod_id_raw = item.get("productId") if item.get("productId") is not None else item.get("idOffer")
        if prod_id_raw is None:
            return jsonify({"success": False, "error": f"Item at index {idx} is missing required 'productId'."}), 400

        try:
            prod_id = int(prod_id_raw)
            if prod_id <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": f"Item at index {idx}: 'productId' must be a positive integer (got {prod_id_raw})."}), 400

        if prod_id in seen_ids:
            return jsonify({"success": False, "error": f"Duplicate productId {prod_id} detected in import batch."}), 400
        seen_ids.add(prod_id)

        name = str(item.get("name") or "").strip()
        if not name:
            return jsonify({"success": False, "error": f"Item at index {idx}: 'name' is required."}), 400

        cat = str(item.get("category") or "").strip().lower()
        col = str(item.get("color") or "").strip().lower()
        thm = str(item.get("theme") or "").strip().lower()
        desc = str(item.get("description") or f"{name} catalog product").strip()

        if not cat and thm:
            cat = thm
        elif not thm and cat:
            thm = cat
        cat, col, thm = determine_product_metadata(name, desc, cat, col, thm)

        try:
            price = float(item.get("Offer_price") if item.get("Offer_price") is not None else item.get("price", 40.0))
            if price < 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": f"Item at index {idx}: 'price' must be a non-negative number."}), 400

        try:
            duration = int(item.get("duration", 30))
            if duration < 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": f"Item at index {idx}: 'duration' must be a non-negative integer."}), 400

        image_name = str(item.get("image_Name") or item.get("image") or "default-product-image.png").strip()
        producer_id = item.get("producer_id")
        if producer_id is not None:
            try:
                producer_id = int(producer_id)
            except (ValueError, TypeError):
                return jsonify({"success": False, "error": f"Item at index {idx}: 'producer_id' must be an integer."}), 400

        validated.append({
            "idOffer": prod_id,
            "name": name,
            "description": desc,
            "Offer_price": price,
            "duration": duration,
            "image_Name": image_name,
            "category": cat,
            "color": col,
            "theme": thm,
            "producer_id": producer_id
        })

    conn = db()
    try:
        # Check database for existing IDs and foreign keys
        for p in validated:
            existing = conn.execute("SELECT idOffer FROM offers WHERE idOffer=?", (p["idOffer"],)).fetchone()
            if existing:
                return jsonify({"success": False, "error": f"Product ID {p['idOffer']} already exists in database. Overwriting is not permitted."}), 409

            if p["producer_id"] is not None:
                p_user = conn.execute("SELECT idCli FROM user WHERE idCli=?", (p["producer_id"],)).fetchone()
                if not p_user:
                    return jsonify({"success": False, "error": f"Producer user ID {p['producer_id']} for product {p['idOffer']} does not exist."}), 400

        with conn:
            for p in validated:
                conn.execute(
                    "INSERT INTO offers (idOffer, description, image_Name, Offer_price, name, duration, category, color, theme, producer_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (p["idOffer"], p["description"], p["image_Name"], p["Offer_price"], p["name"], p["duration"], p["category"], p["color"], p["theme"], p["producer_id"])
                )
    except sqlite3.IntegrityError as e:
        return jsonify({"success": False, "error": f"Database integrity error: {str(e)}"}), 409
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to import products: {str(e)}"}), 500
    finally:
        conn.close()

    csv_res = generate_products_csv()
    return jsonify({
        "success": True,
        "count": len(validated),
        "message": f"Successfully imported {len(validated)} product(s). products.csv has been updated.",
        "csv_result": csv_res
    })


@app.route("/admin/import/ratings", methods=["POST"])
@app.route("/admin/csv/import/ratings", methods=["POST"])
def admin_import_ratings():
    guard = _require_admin()
    if guard:
        return jsonify({"success": False, "error": "Administrator permission required."}), 403

    data, err = _get_request_json()
    if err:
        return jsonify({"success": False, "error": err}), 400

    if isinstance(data, dict) and "ratings" in data:
        data = data["ratings"]

    if not isinstance(data, list):
        return jsonify({"success": False, "error": "Expected a JSON array of rating records."}), 400

    if len(data) == 0:
        return jsonify({"success": False, "error": "JSON array cannot be empty."}), 400

    validated = []
    seen_pairs = set()

    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            return jsonify({"success": False, "error": f"Item at index {idx} must be a JSON object."}), 400

        user_id_raw = item.get("userId") if item.get("userId") is not None else item.get("idCli")
        prod_id_raw = item.get("productId") if item.get("productId") is not None else item.get("idOffer")
        rating_raw = item.get("rating")

        if user_id_raw is None:
            return jsonify({"success": False, "error": f"Item at index {idx} is missing required 'userId'."}), 400
        if prod_id_raw is None:
            return jsonify({"success": False, "error": f"Item at index {idx} is missing required 'productId'."}), 400
        if rating_raw is None:
            return jsonify({"success": False, "error": f"Item at index {idx} is missing required 'rating'."}), 400

        try:
            user_id = int(user_id_raw)
            if user_id <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": f"Item at index {idx}: 'userId' must be a positive integer."}), 400

        try:
            prod_id = int(prod_id_raw)
            if prod_id <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": f"Item at index {idx}: 'productId' must be a positive integer."}), 400

        try:
            if isinstance(rating_raw, bool):
                raise ValueError
            rating = int(rating_raw)
            if not (1 <= rating <= 5):
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": f"Item at index {idx}: 'rating' must be an integer between 1 and 5 (got {rating_raw})."}), 400

        pair = (user_id, prod_id)
        if pair in seen_pairs:
            return jsonify({"success": False, "error": f"Duplicate rating for userId {user_id} and productId {prod_id} in import batch."}), 400
        seen_pairs.add(pair)

        validated.append({
            "idCli": user_id,
            "idOffer": prod_id,
            "rating": rating
        })

    conn = db()
    try:
        # Validate existence of foreign keys and check for existing ratings
        for r in validated:
            user_exists = conn.execute("SELECT idCli FROM user WHERE idCli=?", (r["idCli"],)).fetchone()
            if not user_exists:
                return jsonify({"success": False, "error": f"User ID {r['idCli']} does not exist in database. Please import the user first."}), 400

            offer_exists = conn.execute("SELECT idOffer FROM offers WHERE idOffer=?", (r["idOffer"],)).fetchone()
            if not offer_exists:
                return jsonify({"success": False, "error": f"Product ID {r['idOffer']} does not exist in database. Please import the product first."}), 400

            existing_rating = conn.execute("SELECT idRating FROM ratings WHERE idCli=? AND idOffer=?", (r["idCli"], r["idOffer"])).fetchone()
            if existing_rating:
                return jsonify({"success": False, "error": f"Rating for user {r['idCli']} and product {r['idOffer']} already exists in database. Overwriting is not permitted."}), 409

        now = datetime.now().isoformat()
        with conn:
            for r in validated:
                conn.execute(
                    "INSERT INTO ratings (idCli, idOffer, rating, rated_at) VALUES (?,?,?,?)",
                    (r["idCli"], r["idOffer"], r["rating"], now)
                )
    except sqlite3.IntegrityError as e:
        return jsonify({"success": False, "error": f"Database integrity error: {str(e)}"}), 409
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to import ratings: {str(e)}"}), 500
    finally:
        conn.close()

    csv_res = generate_ratings_csv()
    return jsonify({
        "success": True,
        "count": len(validated),
        "message": f"Successfully imported {len(validated)} rating(s). ratings.csv has been updated.",
        "csv_result": csv_res
    })


@app.route("/admin/import/accounts", methods=["POST"])
@app.route("/admin/csv/import/accounts", methods=["POST"])
def admin_import_accounts():
    guard = _require_admin()
    if guard:
        return jsonify({"success": False, "error": "Administrator permission required."}), 403

    data, err = _get_request_json()
    if err:
        return jsonify({"success": False, "error": err}), 400

    if isinstance(data, dict) and "accounts" in data:
        data = data["accounts"]

    if not isinstance(data, list):
        return jsonify({"success": False, "error": "Expected a JSON array of user accounts."}), 400

    if len(data) == 0:
        return jsonify({"success": False, "error": "JSON array cannot be empty."}), 400

    conn = db()
    try:
        validated = []
        seen_emails = set()
        seen_usernames = set()
        email_regex = r'^[A-Za-z0-9_\-\.]+@[A-Za-z0-9_\-\.]+\.[A-Za-z]{2,}$'
        special_chars = set('#?!@$%^&*-_.~+=<>,;:/\\|()[]{}')

        for idx, item in enumerate(data):
            if not isinstance(item, dict):
                return jsonify({"success": False, "error": f"Item at index {idx} must be a JSON object."}), 400

            username = str(item.get("username") or item.get("FirstName") or "").strip()
            email = str(item.get("email") or item.get("Email") or "").strip().lower()
            password = str(item.get("password") or item.get("Password") or "")
            fav_theme = str(item.get("favTheme") or item.get("theme") or "").strip().lower()
            fav_color = str(item.get("favColor") or item.get("color") or "").strip().lower()

            if not username:
                return jsonify({"success": False, "error": f"Item at index {idx}: 'username' is required."}), 400
            if not email:
                return jsonify({"success": False, "error": f"Item at index {idx}: 'email' is required."}), 400
            if not password:
                return jsonify({"success": False, "error": f"Item at index {idx}: 'password' is required."}), 400

            # Validate email format
            if not re.match(email_regex, email):
                return jsonify({"success": False, "error": f"Item at index {idx}: Invalid email format '{email}'."}), 400

            # Validate password according to registration rules
            if len(password) < 8:
                return jsonify({"success": False, "error": f"Item at index {idx}: Password for '{username}' must be at least 8 characters long."}), 400
            if not any(c.islower() for c in password):
                return jsonify({"success": False, "error": f"Item at index {idx}: Password for '{username}' requires at least one lowercase character."}), 400
            if not any(c.isupper() for c in password):
                return jsonify({"success": False, "error": f"Item at index {idx}: Password for '{username}' requires at least one uppercase character."}), 400
            if not any(c.isdigit() for c in password):
                return jsonify({"success": False, "error": f"Item at index {idx}: Password for '{username}' requires at least one number."}), 400
            if not any(c in special_chars for c in password):
                return jsonify({"success": False, "error": f"Item at index {idx}: Password for '{username}' requires at least one special character."}), 400

            # In-batch duplicate checks
            if email in seen_emails:
                return jsonify({"success": False, "error": f"Duplicate email '{email}' detected in import batch."}), 400
            if username.lower() in seen_usernames:
                return jsonify({"success": False, "error": f"Duplicate username '{username}' detected in import batch."}), 400
            seen_emails.add(email)
            seen_usernames.add(username.lower())

            # Database uniqueness checks
            existing_email = conn.execute("SELECT idCli FROM user WHERE LOWER(Email)=?", (email,)).fetchone()
            if existing_email:
                return jsonify({"success": False, "error": f"Email '{email}' is already registered in database (User ID: {existing_email['idCli']})."}), 409

            existing_username = conn.execute("SELECT idCli FROM user WHERE LOWER(FirstName)=?", (username.lower(),)).fetchone()
            if existing_username:
                return jsonify({"success": False, "error": f"Username '{username}' already exists in database (User ID: {existing_username['idCli']})."}), 409

            # Hash the password (never store plaintext)
            hashed_pw = generate_password_hash(password)

            validated.append({
                "FirstName": username,
                "LastName": "User",
                "Email": email,
                "Password": hashed_pw,
                "raw_username": username,
                "status": "verified",
                "role": "client",  # Strictly force client role
                "Date_Joined": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "whatsapp": "",
                "pfpName": "user583abc_1649114257.png",
                "favTheme": fav_theme,
                "favColor": fav_color
            })

        created_users = []
        with conn:
            for u in validated:
                cur = conn.execute(
                    "INSERT INTO user (FirstName, LastName, Email, Password, Date_Joined, whatsapp, status, role, pfpName, favTheme, favColor) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (u["FirstName"], u["LastName"], u["Email"], u["Password"], u["Date_Joined"], u["whatsapp"], u["status"], u["role"], u["pfpName"], u["favTheme"], u["favColor"])
                )
                user_id = cur.lastrowid
                conn.execute("INSERT OR IGNORE INTO cart (idCli) VALUES (?)", (user_id,))
                created_users.append({
                    "userId": user_id,
                    "username": u["raw_username"],
                    "email": u["Email"],
                    "role": u["role"],
                    "favTheme": u["favTheme"],
                    "favColor": u["favColor"]
                })
    except sqlite3.IntegrityError as e:
        return jsonify({"success": False, "error": f"Database integrity error: {str(e)}"}), 409
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to create accounts: {str(e)}"}), 500
    finally:
        conn.close()

    csv_res = generate_users_csv()
    return jsonify({
        "success": True,
        "count": len(created_users),
        "message": f"Successfully created {len(created_users)} user account(s).",
        "created_users": created_users,
        "csv_result": csv_res
    })


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
