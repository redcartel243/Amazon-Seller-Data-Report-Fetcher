# Amazon-Seller-Data-Report-Fetcher
A Flask application that simplifies the process of requesting and downloading reports from the Amazon Seller Partner API and Amazon Ads API. It supports OAuth2 authentication, token caching, and integration simple integration with HTTPS. The app fetches Sponsored Products and Sponsored Display reports and caches profile data to enhance efficiency.


# Amazon Seller Data Report Fetcher

## Introduction
This Flask app simplifies downloading reports from the Amazon Seller Partner API and Amazon Ads API, aiding integration with tools like Power BI.

## Key Features
- OAuth2 authentication
- Token and profile caching
- Supports Sponsored Products (SP) and Sponsored Display (SD) reports
- Easy Power BI integration

## Setup and Installation
1. **Clone the Repository**
    ```sh
    git clone https://github.com/yourusername/amazon-advertising-report-middleware.git
    cd amazon-advertising-report-middleware
    ```

2. **Set Up Virtual Environment**
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows: `venv\Scripts\activate`
    ```

3. **Install Dependencies**
    ```sh
    pip install -r requirements.txt
    ```

4. **Configure Environment Variables**
    Create a `.env` file in the project directory with the following content:
    ```
    AD_API_CLIENT_ID=<your_client_id>
    AD_API_CLIENT_SECRET=<your_client_secret>
    ```

5. **Initialize the Database**
    ```sh
    python -c "from app import init_db; init_db()"
    ```

6. **Run the Application**
    ```sh
    python app.py
    ```

## How to Use
### Authorization
Navigate to `http://127.0.0.1:5000/authorize` to initiate the OAuth2 authorization process.

### Fetch Profiles
Call the endpoint `GET /get-profiles` to retrieve and cache your Amazon Advertising profiles.

### Request Reports
Use the `GET /get-ad-report` endpoint to request specific advertising reports. 

#### Example Request
To request a Sponsored Display report:


## Contributing
1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes.
4. Commit your changes (`git commit -am 'Add new feature'`).
5. Push to the branch (`git push origin feature-branch`).
6. Create a new Pull Request.

## License
This project is licensed under the MIT License. See the `LICENSE` file for more details.
