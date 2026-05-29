# README

> This is the backend project for the Poway Symphony Orchestra prototype page depicted in the wi-pages repository. The backend supports this by:
- having a variety of APIs, such as for logging in, chatting with admin, creating and storing member cards, and more, to support the functionality of the PSO website.

Template from Open Coding Society, expanded upon for the needs of our project. 

## MIT License

Copyright (c) 2026 Open Coding Society

Permission has been granted by Open Coding Society to use this template for the use of this project. 

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

# Flask Portfolio Starter (Simplified)

This project helps you create and run a Flask web server.

* GitHub Repository: [flask repository](https://github.com/open-coding-society/flask?utm_source=chatgpt.com)
* You can:

  * **Use this as a template** to make your own copy
  * **Fork the repository** if you want to contribute through pull requests

---

# Getting Started

These steps work on macOS, Ubuntu, or WSL with Python 3.9+ installed.

## 1. Clone the Project

Open Terminal and run:

```bash
mkdir -p ~/openccs
cd ~/openccs

git clone https://github.com/open-coding-society/flask.git

cd flask
```

---

## 2. Set Up Python

Create and activate a virtual environment, then install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

# Open the Project in VSCode

Launch VSCode from the terminal:

```bash
code .
```

## Configure Python Interpreter

1. Open Command Palette:

   * `Ctrl + Shift + P` (Windows/Linux)
   * `Cmd + Shift + P` (Mac)

2. Search for:

   * `Python: Select Interpreter`

3. Select:

   ```bash
   ./venv/bin/python
   ```

You can verify the correct interpreter using:

```bash
which python
```

---

# Install Helpful VSCode Extension

Install the **SQLite3 Editor** extension from the Extensions Marketplace.

This lets you open and view the database file:

```bash
instance/volumes/user_management.db
```

---

# Create a `.env` File

In the project root folder, create a file named:

```bash
.env
```

This file stores passwords, API keys, and configuration settings.

Example:

```env
DEFAULT_PASSWORD='123Qwerty!'
ADMIN_USER='Thomas Edison'
ADMIN_UID='toby'
ADMIN_PASSWORD='123Toby!'
```

You can also add API keys for services like Gemini, Groq, GitHub, or AWS.

---

# Initialize the Database

Run:

```bash
./scripts/db_init.py
```

This creates the database and starter data.

Afterward, you can open:

```bash
instance/volumes/user_management.db
```

and view the `users` table using the SQLite extension.

---

# Run the Project

1. Open `main.py`
2. Click the ▶ Run button in VSCode
3. Open the localhost link shown in the terminal

Usually:

```bash
http://localhost:8587
```

Log in using the usernames and passwords from your `.env` file.

---

# Test the API

Example endpoint:

```bash
http://localhost:8587/api/jokes/
```

---

# Main User API Endpoints

| Purpose             | Endpoint            |
| ------------------- | ------------------- |
| Login               | `/api/authenticate` |
| Get Current User    | `/api/id`           |
| Create Account      | `/api/user`         |
| Get Posts           | `/api/post/all`     |
| Create Post         | `/api/post`         |
| Chat with Gemini AI | `/api/gemini`       |

---

# MicroBlog API

## Posts

| Method | Endpoint         | Purpose       |
| ------ | ---------------- | ------------- |
| POST   | `/api/microblog` | Create a post |
| GET    | `/api/microblog` | Get posts     |
| PUT    | `/api/microblog` | Update a post |
| DELETE | `/api/microblog` | Delete a post |

## Optional Query Parameters

```bash
?topicId=1
?userId=123
?search=flask
?limit=20
```

---

# MicroBlog Interactions

| Method | Endpoint                  | Purpose         |
| ------ | ------------------------- | --------------- |
| POST   | `/api/microblog/reply`    | Reply to a post |
| POST   | `/api/microblog/reaction` | Add reaction    |
| DELETE | `/api/microblog/reaction` | Remove reaction |

---

# Page Integration

| Method | Endpoint                            | Purpose           |
| ------ | ----------------------------------- | ----------------- |
| GET    | `/api/microblog/page/<page_key>`    | Get page posts    |
| POST   | `/api/microblog/topics/auto-create` | Create page topic |
| GET    | `/api/microblog/topics?pagePath=X`  | Get topic by page |

---

# Additional Resources

* [Python/Flask Guide](https://pages.opencodingsociety.com/python/flask?utm_source=chatgpt.com)
* [Legacy Flask Intro](https://pages.opencodingsociety.com/flask-overview?utm_source=chatgpt.com)


