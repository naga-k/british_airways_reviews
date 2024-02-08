import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time

def scrape_page(page_num):
    base_url = "https://www.airlinequality.com/airline-reviews/british-airways"
    page_size = 100
    
    # Create URL for the specified page
    url = f"{base_url}/page/{page_num}/?sortby=post_date%3ADesc&pagesize={page_size}"

    # Collect HTML data from this page
    response = requests.get(url)

    # Parse content
    content = response.content
    parsed_content = BeautifulSoup(content, 'html.parser')
    
    # Extracting reviews
    review_articles = parsed_content.find_all("article", itemprop="review")

    reviews = []
    for review_article in review_articles:
        review = {}

        # Extract Date Published
        date_published_tag = review_article.find("meta", itemprop="datePublished")
        review['date_published'] = date_published_tag['content'] if date_published_tag else None

        # Extract Rating Value
        rating_value_tag = review_article.find("span", itemprop="ratingValue")
        review['rating_value'] = rating_value_tag.text.strip() if rating_value_tag else None

        # Extract Author Name
        author_name_tag = review_article.find("span", itemprop="name")
        review['author_name'] = author_name_tag.text.strip() if author_name_tag else None

        # Extract Author Location
        author_info_tag = review_article.find("h3", class_="text_sub_header userStatusWrapper")
        review['author_location'] = author_info_tag.text.split('(')[-1].strip(')') if author_info_tag else None

        # Extract Review Text
        review_text_tag = review_article.find("div", itemprop="reviewBody")
        review['review_text'] = review_text_tag.text.strip() if review_text_tag else None

        # Extract Verified Information
        verified_tag = review_article.find("div", class_="tc_mobile")
        if verified_tag and verified_tag.find("strong"):
            verified_text = verified_tag.find("strong").text.strip() if verified_tag else None
            if verified_text:
                review['verified'] = verified_text if "verified" in verified_text.lower() else None
        # Extract Review Ratings
        ratings = {}
        review_ratings_table = review_article.find("table", class_="review-ratings")
        if review_ratings_table:
            for row in review_ratings_table.find_all("tr"):
                header = row.find("td", class_="review-rating-header")
                if header:
                    header_text = header.text.strip().lower().replace(' ', '_')
                    rating_stars = row.find("td", class_="review-rating-stars")
                    if rating_stars:
                        stars = rating_stars.find_all("span", class_="star fill")
                        value = len(stars)
                    else:
                        value_element = row.find("td", class_="review-value")
                        if value_element:
                            value = value_element.text.strip()
                        else:
                            value = None
                    ratings[header_text] = value


        review.update(ratings)
        reviews.append(review)
    
    return reviews

def main():
    pages = 38  # Number of pages to scrape

    # Initialize list to store DataFrames
    dfs = []

    # Start parallel scraping
    with ThreadPoolExecutor() as executor:
        # Submit scraping tasks for each page
        futures = [executor.submit(scrape_page, page_num) for page_num in range(1, pages + 1)]
        
        # Gather results
        for future in futures:
            reviews = future.result()
            df = pd.DataFrame(reviews)
            dfs.append(df)

    # Concatenate DataFrames
    final_df = pd.concat(dfs, ignore_index=True)

    # Save DataFrame to CSV
    final_df.to_csv('/project/jacobcha/nk643/webscraping/british_airways_reviews.csv', index=False)
    print("CSV saved successfully.")

if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"Time taken: {time.time() - start_time} seconds.")
