import os
import pickle
import random
import time


def human_sleep(min_sec=6, max_sec=12):
    t = random.uniform(min_sec, max_sec)
    print(f"⏳ Human sleep: {round(t, 2)} sec")
    time.sleep(t)

def short_sleep():
    human_sleep(3,8)

def scroll_page(driver, duration=20, step=800):
    start = time.time()
    y = 0
    while time.time() - start < duration:
        driver.execute_script(f"window.scrollTo(0, {y});")
        y += step
        time.sleep(random.uniform(0.8, 1.5))

def load_cookies(driver, cookies_file):
    if not os.path.exists(cookies_file):
        return False

    driver.get("https://www.linkedin.com")
    short_sleep()

    try:
        cookies = pickle.load(open(cookies_file, "rb"))
        for c in cookies:
            c.pop("sameSite", None)
            try:
                driver.add_cookie(c)
            except:
                pass

        driver.get("https://www.linkedin.com/feed/")
        human_sleep()
        return True
    except:
        return False

def save_cookies(driver, cookies_file):
    pickle.dump(driver.get_cookies(), open(cookies_file, "wb"))



