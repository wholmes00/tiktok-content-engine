"""
Centralized Supabase client for the TikTok Content Engine.

All pipeline modules import the client from here instead of
creating their own. This keeps the connection config in one place.

The anon key below is a PUBLIC read-only key — it cannot modify
or delete data. It's safe for a private repo but should be moved
to an environment variable if the repo ever becomes public.
"""

import os
from supabase import create_client

SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://owklfaoaxdrggmbtcwpn.supabase.co",
)
SUPABASE_KEY = os.environ.get(
    "SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im93a2xmYW9heGRyZ2dtYnRjd3BuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM0NDQyNjcsImV4cCI6MjA4OTAyMDI2N30.EQkJzeS4MYG4QO6aH9c_zbF7BNuH_bKwZIKQpTXvw1Y",
)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
