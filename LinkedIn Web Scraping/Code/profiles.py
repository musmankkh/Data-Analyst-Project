from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import pickle
import os
import random

# ======================================================
# HUMAN-LIKE BEHAVIOR HELPERS
# ======================================================

def human_sleep(min_sec=6, max_sec=12):
    t = random.uniform(min_sec, max_sec)
    print(f"⏳ Human sleep: {round(t, 2)} sec")
    time.sleep(t)

def short_sleep():
    human_sleep(2, 4)

def scroll_page(driver, duration=20, step=800):
    start = time.time()
    y = 0
    while time.time() - start < duration:
        driver.execute_script(f"window.scrollTo(0, {y});")
        y += step
        time.sleep(random.uniform(0.8, 1.5))

# ======================================================
# COOKIE MANAGEMENT
# ======================================================

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

def extract_text(tag):
    return tag.get_text(strip=True) if tag else ""

# ======================================================
# ABOUT EXTRACTOR
# ======================================================
def extract_about(soup, driver=None):
    # 🔵 1) Try to click "See more" only if driver is provided
    if driver:
        see_more_buttons = driver.find_elements(By.CSS_SELECTOR, ".inline-show-more-text__button")
        if see_more_buttons:
            try:
                driver.execute_script("arguments[0].click();", see_more_buttons[0])
                time.sleep(1)
                soup = BeautifulSoup(driver.page_source, "lxml")
            except:
                pass  # If click fails, still fallback to soup extraction

    # 🔵 2) Locate the About section by ID or header text
    about_anchor = soup.find("div", id="about")

    if about_anchor:
        about_section = about_anchor.find_parent("section")
    else:
        about_header = soup.find("h2", string=lambda t: t and "About" in t)
        if about_header:
            about_section = about_header.find_parent("section")
        else:
            return ""

    if not about_section:
        return ""

    # 🔵 3) Extract text from the <span aria-hidden="true"> (LinkedIn uses these for full text)
    text_spans = about_section.find_all("span", attrs={"aria-hidden": "true"})
    for span in text_spans:
        text = span.get_text(" ", strip=True)
        if len(text) > 40:
            return " ".join(text.split())

    # 🔵 4) Fallback: remove unwanted elements and extract all visible text
    about_copy = BeautifulSoup(str(about_section), 'lxml')
    for element in about_copy.find_all(["button", "a", "svg"]):
        element.decompose()

    text = " ".join(about_copy.get_text(" ", strip=True).split())
    return text if len(text) > 40 else ""


# =====================================================
# EXPERIENCE EXTRACTOR
# ======================================================
def extract_experience(soup):
    experience = []
    
    # Find all experience list items
    experience_items = soup.find_all("li", class_=lambda c: c and "pvs-list__paged-list-item" in c)
    
    for item in experience_items:
        # Skip if this is a nested role within a parent company
        if "pvs-list__item--with-top-padding" in item.get("class", []):
            continue
            
        # Find all spans with aria-hidden="true" which contain the data
        spans = item.find_all("span", attrs={"aria-hidden": "true"})
        
        if len(spans) < 2:
            continue
            
        # Extract role (first bold span)
        role = extract_text(spans[0]) if len(spans) > 0 else ""
        
        # Extract company and employment type (second span)
        company_line = extract_text(spans[1]) if len(spans) > 1 else ""
        # Split by '·' to separate company from employment type
        company = company_line.split("·")[0].strip()
        
        # Extract dates (third span)
        dates = extract_text(spans[2]) if len(spans) > 2 else ""
        
        # Extract location (fourth span, if exists)
        location = extract_text(spans[3]) if len(spans) > 3 else ""
        
        experience.append({
            "Company": company,
            "Role": role,
            "Dates": dates,
            "Location": location
        })
    
    return experience

# ======================================================
# EDUCATION EXTRACTOR
# ======================================================

def extract_educations(soup):
    education = []
    
    # Find the education section
    educations = None
    sections = soup.find_all('section')
    
    for sec in sections:
        if sec.find('div', {'id': 'education'}):
            educations = sec
            break
    
    if not educations:
        print("⚠ No education section found")
        return education
    
    # Find all education list items
    items = educations.find_all('li', {'class': 'artdeco-list__item SYxxFBCsJjLTMbrokUEBUcUlQlXeSC oSEHXmMWhAcdNWQrLAOHUmVVfZqaFcOjwpWLmQ'})
    
    if not items:
        print("⚠ No education items found")
        return education
    
    for item in items:
        # Find the school name in the div with hoverable-link-text t-bold classes
        school_div = item.find('div', {'class': lambda c: c and 'hoverable-link-text' in c and 't-bold' in c})
        school_span = school_div.find('span', {'aria-hidden': 'true'}) if school_div else None
        school = school_span.get_text().strip() if school_span else ""
        
        # Find the degree in the span with t-14 t-normal classes
        degree_span = item.find('span', {'class': 't-14 t-normal'})
        degree_text = degree_span.find('span', {'aria-hidden': 'true'}) if degree_span else None
        degree = degree_text.get_text().strip() if degree_text else ""
        
        if school:
            education.append(f"{school} — {degree}")
    
    return education


def extract_education(soup):
    education = []
    
    # Find the main education list container
    edu_ul = soup.find("ul", attrs={
        "class": lambda c: c and "MfnJWTKvonmCRjPCFbtrYKGCxqHNKhpqVw" in c
    })
    
    if not edu_ul:
        print("⚠ No education list found")
        return education
    
    # Find all top-level <li> items
    edu_items = edu_ul.find_all("li", recursive=False)
    
    for li in edu_items:
        # School name is in a <span> inside a div with class "hoverable-link-text t-bold"
        school_div = li.find("div", attrs={
            "class": lambda c: c and "hoverable-link-text" in c and "t-bold" in c
        })
        school = extract_text(school_div) if school_div else ""
        
        # Degree is in a <span> with class "t-14 t-normal"
        degree_span = li.find("span", attrs={
            "class": lambda c: c and "t-14" in c and "t-normal" in c
        })
        degree = extract_text(degree_span) if degree_span else ""
        
        if school:
            education.append(f"{school} — {degree}")
    
    return education


# ======================================================
# CERTIFICATION EXTRACTOR
# ======================================================

def extract_certifications(driver=None, soup=None):
    """
    Extract LinkedIn certifications from the page.
    If driver is provided, tries to click "See all" first.
    """
    certifications = []

    # 🔵 1) Click "See all" if driver is provided
    if driver:
        try:
            show_all_btn = driver.find_elements(By.ID, "navigation-index-see-all-licenses-and-certifications")
            if show_all_btn:
                driver.execute_script("arguments[0].click();", show_all_btn[0])
                time.sleep(1)
                # Optionally scroll to load more certifications
                scroll_page(driver, duration=6)
                soup = BeautifulSoup(driver.page_source, "lxml")
                print("✓ 'See all certifications' button clicked.")
        except:
            print("⚠ Could not click 'See all certifications'. Proceeding with visible items.")

    # 🔵 2) If soup not provided, try using driver page source
    if soup is None and driver:
        soup = BeautifulSoup(driver.page_source, "lxml")
    elif soup is None:
        return certifications  # Nothing to extract

    # 🔵 3) Extract certifications
    cert_items = soup.find_all("li", attrs={"class": lambda c: c and "pvs-list__paged-list-item" in c})
    for item in cert_items:
        title_tag = item.find("div", class_=lambda c: c and "t-bold" in c)
        org_tag = item.find("span", class_="t-14 t-normal")

        if title_tag and org_tag:
            title = " ".join(title_tag.get_text(" ", strip=True).split())
            org = " ".join(org_tag.get_text(" ", strip=True).split())
            certifications.append({"Title": title, "Organization": org})

    print(f"✓ Certifications extracted: {len(certifications)}")
    return certifications




# ======================================================
# DYNAMIC EXCEL SAVE - PROPERLY HANDLES ALL FIELDS
# ======================================================

def save_all_profiles(all_profiles_data, filename=r"C:\Users\Usman Asghar\Desktop\Intern\LinkedIn Web Scraping\Excel File\Profiles_data.xlsx"):
    
    # Check if we have any data
    if not all_profiles_data:
        print("❌ No profile data to save!")
        return
    
    print(f"\n📊 Preparing to save {len(all_profiles_data)} profiles...")
    
    # Ensure directory exists
    output_dir = os.path.dirname(filename)
    if not os.path.exists(output_dir):
        print(f"📁 Creating directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Calculate max counts
        max_education = max((len(p.get("education", [])) for p in all_profiles_data), default=0)
        max_experiences = max((len(p.get("experience", [])) for p in all_profiles_data), default=0)
        max_certifications = max((len(p.get("certifications", [])) for p in all_profiles_data), default=0)
        
        print(f"   Max Education: {max_education}")
        print(f"   Max Experience: {max_experiences}")
        print(f"   Max Certifications: {max_certifications}")

        rows = []

        for idx, profile in enumerate(all_profiles_data, 1):
            print(f"   Processing profile {idx}/{len(all_profiles_data)}...")
            
            row = {
                "Name": profile.get("name", "null") or "null",
                "Headline": profile.get("headline", "null") or "null",
                "About": profile.get("about", "null") or "null",
                "Profile URL": profile.get("url", "null") or "null"
            }

            # EDUCATION
            edu_list = profile.get("education", [])
            for i in range(1, max_education + 1):
                row[f"Education {i}"] = edu_list[i-1] if i <= len(edu_list) else "null"

            # EXPERIENCE
            exp_list = profile.get("experience", [])
            for i in range(1, max_experiences + 1):
                if i <= len(exp_list):
                    exp = exp_list[i-1]
                    row[f"Exp {i} - Company"] = exp.get("Company", "null") or "null"
                    row[f"Exp {i} - Role"] = exp.get("Role", "null") or "null"
                    row[f"Exp {i} - Dates"] = exp.get("Dates", "null") or "null"
                    row[f"Exp {i} - Location"] = exp.get("Location", "null") or "null"
                else:
                    row[f"Exp {i} - Company"] = "null"
                    row[f"Exp {i} - Role"] = "null"
                    row[f"Exp {i} - Dates"] = "null"
                    row[f"Exp {i} - Location"] = "null"

            # CERTIFICATIONS
            cert_list = profile.get("certifications", [])
            for i in range(1, max_certifications + 1):
                if i <= len(cert_list):
                    cert = cert_list[i - 1]
                    row[f"Cert {i} - Title"] = cert.get("Title", "null") or "null"
                    row[f"Cert {i} - Organization"] = cert.get("Organization", "null") or "null"
                else:
                    row[f"Cert {i} - Title"] = "null"
                    row[f"Cert {i} - Organization"] = "null"

            rows.append(row)

        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Reorder columns
        base_cols = ["Name", "Headline", "About", "Profile URL"]
        edu_cols = sorted([c for c in df.columns if c.startswith("Education")])
        exp_cols = sorted([c for c in df.columns if c.startswith("Exp")])
        cert_cols = sorted([c for c in df.columns if c.startswith("Cert")])

        df = df[base_cols + edu_cols + exp_cols + cert_cols]
        
        # Save to Excel
        df.to_excel(filename, index=False, engine="openpyxl")
        print(f"\n✅ SUCCESS: File saved to {filename}")
        print(f"   Total rows: {len(df)}")
        print(f"   Total columns: {len(df.columns)}")
        
    except PermissionError:
        print(f"\n❌ ERROR: Permission denied. Close the Excel file if it's open!")
        print(f"   File: {filename}")
    except Exception as e:
        print(f"\n❌ ERROR saving Excel file: {str(e)}")
        print(f"   File: {filename}")
        
        # Try saving to a backup location
        backup_file = os.path.join(os.path.dirname(filename), "Profiles_data_BACKUP.xlsx")
        try:
            df.to_excel(backup_file, index=False, engine="openpyxl")
            print(f"✅ Saved to backup location: {backup_file}")
        except:
            print(f"❌ Backup save also failed!")

# ======================================================
# MAIN SCRIPT
# ======================================================

def main():
    df = pd.read_excel(r"your input path")
    cookies_file = "linkedins_cookies.pkl"

    options = Options()
    options.add_argument("--start-maximized")


    driver = webdriver.Chrome(options=options)

    # LOGIN
    if load_cookies(driver, cookies_file):
        print("✔ Logged in using saved cookies.")
    else:
        print("⚠ Manual login required.")
        driver.get("https://www.linkedin.com/login")
        short_sleep()
        input("👉 Log in manually, then press ENTER...")
        human_sleep()
        save_cookies(driver, cookies_file)
        print("✔ Cookies saved!")

    human_sleep()

    # ======================================================
    # PROCESS ALL PROFILES
    # ======================================================

    all_profiles_data = []

    for index, row in df.iterrows():
        profile_url = row["Profile URL"]
        print("\n" + "="*70)
        print(f"PROCESSING [{index+1}/{len(df)}]: {profile_url}")
        print("="*70)

        profile_data = {
            "url": profile_url,
            "name": "",
            "headline": "",
            "about": "",
            "education": [],
            "experience": [],
            "certifications": []
        }

        try:
            # Load main profile page
            driver.get(profile_url)
            human_sleep()
            scroll_page(driver, duration=10)
            short_sleep()

            soup = BeautifulSoup(driver.page_source, "lxml")

            # BASIC INFO
            profile_data["name"] = extract_text(soup.find("h1"))
            headline_tag = soup.find("div", class_=lambda c: c and "text-body-medium" in c)
            profile_data["headline"] = extract_text(headline_tag)

            print("✓ Name:", profile_data["name"])
            print("✓ Headline:", profile_data["headline"])

            # ABOUT
            profile_data["about"] = extract_about(soup, driver)
            print("✓ About:", profile_data["about"])

           # EDUCATION
            print("➡ Extracting Education...")

            edu_buttons = driver.find_elements(By.ID, "navigation-index-see-all-education")

            if edu_buttons:
                print("🔵 'See all education' button found — opening full section...")
                driver.execute_script("arguments[0].click();", edu_buttons[0])
                human_sleep()
                scroll_page(driver, duration=5)
                
                edu_soup = BeautifulSoup(driver.page_source, "lxml")
                profile_data["education"] = extract_education(edu_soup)

                print(f"✓ Education extracted (full view): {len(profile_data['education'])} items")
                print("✓ Education:", profile_data["education"])

            else:
                print("🟡 No 'See all education' button — extracting from main profile...")
                # Use the ALREADY CREATED soup object from the main profile page
                profile_data["education"] = extract_educations(soup)
                print(f"✓ Education extracted from main view: {len(profile_data['education'])} items")
                print("✓ Education:", profile_data["education"])    

            # EXPERIENCE
            driver.get(profile_url)
            human_sleep()
            scroll_page(driver, duration=10)

            # Create a fresh soup for the reloaded page
            main_soup = BeautifulSoup(driver.page_source, "lxml")

            print("➡ Extracting Experience...")

            exp_buttons = driver.find_elements(By.ID, "navigation-index-see-all-experiences")

            if exp_buttons:
                print("🔵 'See all experiences' button found — opening full section...")
                driver.execute_script("arguments[0].click();", exp_buttons[0])
                human_sleep()
                scroll_page(driver, duration=15)

                exp_soup = BeautifulSoup(driver.page_source, "lxml")
                profile_data["experience"] = extract_experience(exp_soup)

                print(f"✓ Experience extracted from full view: {len(profile_data['experience'])} items")
                print("✓ Exp:", profile_data["experience"])

            else:
                print("🟡 No 'See all experiences' button — extracting from main profile...")
                # Use the soup created AFTER reloading and scrolling the page
                profile_data["experience"] = extract_experience(main_soup)
                print(f"✓ Experience extracted from main view: {len(profile_data['experience'])} items")
                print("✓ Exp:", profile_data["experience"])
                

            # RETURN TO MAIN PAGE BEFORE CERTIFICATIONS
            print("↩️ Returning to main profile...")
            driver.get(profile_url)
            human_sleep()
            scroll_page(driver, duration=8)

            # CERTIFICATIONS
            try:
                print("➡ Extracting Certifications...")
                profile_data["certifications"] = extract_certifications(driver)
                print(f"✓ Certifications extracted: {len(profile_data['certifications'])} items")
            except Exception as e:
                print(f"⚠ No certifications found: {str(e)}")

        except Exception as e:
            print(f"❌ Error processing profile: {str(e)}")

        # SAVE PROFILE (even if some sections failed)
        all_profiles_data.append(profile_data)

        if index < len(df) - 1:
            print("⏸ Waiting before next profile...")
            human_sleep(8, 15)

    # FINAL SAVE
    save_all_profiles(all_profiles_data)

    print("\n✔ SCRAPING COMPLETE!")
    print(f"✔ Processed {len(all_profiles_data)} profiles\n")

    driver.quit()

if __name__ == "__main__":
    main()