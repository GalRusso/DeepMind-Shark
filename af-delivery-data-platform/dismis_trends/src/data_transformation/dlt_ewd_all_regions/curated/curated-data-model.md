## Fact Tables

### 1. fact_trends
**Purpose**: Central fact table for comprehensive trend analysis
**Partition**: `internal_date_reported_key`

**Key Metrics**:
- `days_to_msm`: Days from internal report date to mainstream media coverage
- `platform_spread_count`: Number of platforms trend spread to
- `google_products_impacted_count`: Number of Google products potentially impacted
- `related_trends_count`: Number of related trends
- `keywords_count`: Number of keywords
- `content_tags_count`: Number of content tags
- `risk_estimation`: Original risk estimation text
- `reach_estimation`: Original reach estimation text

**Dimension Keys**:
- `internal_date_reported_key`: Internal date when the trend was reported
- `external_date_reported_key`: Date when client provided feedback on labeled data
- `date_published_key`: Publish date of example source links
- `msm_date_key`: Date when mainstream media reported on the trend
- `msm_key`: Mainstream media outlet identifier
- `feedback_date_key`: Date when client provided feedback
- `source_platform_key`: Platform where the trend was first identified
- `trend_type_key`: Trend type and risk categorization identifier
- `feedback_key`: Client feedback classification identifier

**Flags**:
- `has_msm_coverage`: Mainstream media coverage flag
- `has_client_feedback`: Client feedback flag
- `has_google_product_impact`: Google product impact flag
- `has_multiple_platforms`: Multi-platform spread flag
- `has_screenshots`: Screenshot presence flag

**Natural Keys**:
- `trend_id`: Primary trend identifier
- `external_trend_id`: External system trend ID

**Descriptive Attributes**:
- `trend_name`: Trend name
- `trend_description`: Trend description
- `primary_language`: Primary language used for this trend
- `additional_information`: Additional information including secondary countries and languages
- `example_links`: Example links to the source (array)
- `screenshot_media_links`: Actual media link of the screenshots (array)
- `screenshot_folder_link`: Google Drive folder link to the screenshots
- `msm_link`: Mainstream media article link
- `analyst`: Analyst name
- `country`: Country where trend was identified
- `region`: Region (US, EMEA, APAC)

### 2. fact_copies
**Purpose**: Artifact/copies fact table for artifact analysis
**Partition**: `date_reported_key`

**Key Metrics**:
- `screenshot_count`: Number of screenshots

**Dimension Keys**:
- `date_reported_key`: Date when the trend was reported
- `host_platform_key`: Platform where the artifact is hosted
- `artifact_status_key`: Status of the artifact (online/offline/restricted)

**Flags**:
- `has_artifact_link`: True if artifact link is present
- `has_screenshots`: True if screenshots are available
- `is_google_hosted`: True if artifact is hosted on Google platform

**Natural Keys**:
- `monday_item_id`: Monday.com item ID (primary key)
- `trend_id`: Related trend ID
- `external_trend_id`: External trend ID
- `external_artifact_id`: External artifact ID

**Descriptive Attributes**:
- `trend_name`: Trend name
- `artifact_link`: Link to the artifact
- `screenshot_media_links`: Actual media links of the screenshots (array)
- `addtional_notes`: Additional notes about the artifact/copy (note: field name has typo but preserved for compatibility)
- `analyst`: Analyst name
- `country`: Country where trend was identified
- `region`: Region (US, EMEA, APAC)

## Dimension Tables

### 1. dim_date
**Purpose**: Time dimension for temporal analysis
**Coverage**: 2020-2030

**Key Attributes**:
- `date_key`: Primary key (DateType)
- `year`, `quarter`, `month`: Temporal hierarchies
- `month_name`, `month_short`: Month descriptions
- `week_of_year`, `day_of_month`, `day_of_week`: Granular time attributes
- `day_name`, `day_short`: Day descriptions
- `is_weekend`, `is_month_start`, `is_month_end`: Business flags
- `fiscal_year`, `fiscal_quarter`: Fiscal calendar
- `year_month`, `year_quarter`: Formatted periods

### 2. dim_platform
**Purpose**: Platform dimension for platform-based analysis

**Key Attributes**:
- `platform_key`: Surrogate key
- `platform_name`: Platform name
- `platform_category`: Category (Google Product, Social Media, News Media, Other)
- `is_google_product`: Google product flag
- `is_social_media`: Social media flag
- `platform_hash`: Hash for natural key

**Categories**:
- **Google Product**: YouTube, Gmail, Drive, Docs, Sheets, Android, Chrome, Search, Maps, Play
- **Social Media**: Facebook, Twitter, Instagram, TikTok, Reddit, Telegram, WhatsApp, Discord, Snapchat, LinkedIn
- **News Media**: News outlets, media companies, press organizations

### 4. dim_artifact_status
**Purpose**: Artifact status dimension for actionability analysis

**Key Attributes**:
- `artifact_status_key`: Surrogate key
- `artifact_status`: Raw status value
- `status_category`: Categorized status (Online, Offline, Restricted, Other)
- `is_actionable`: Actionable flag (restricted/offline)
- `is_active`: Active/online flag
- `status_hash`: Hash for natural key

### 5. dim_client_feedback
**Purpose**: Client feedback dimension for feedback analysis

**Key Attributes**:
- `feedback_key`: Surrogate key
- `client_feedback`: Raw feedback text
- `feedback_category`: Categorized feedback
- `is_relevant_actionable`: Relevant & actionable flag
- `is_positive_feedback`: Positive feedback flag
- `feedback_hash`: Hash for natural key

**Categories**:
- Relevant & Actionable
- Relevant But Already Known
- Flag Accepted, Relevant
- Flag Rejected, Not Relevant
- Not Relevant
- Format Error

### 6. dim_mainstream_media
**Purpose**: Mainstream media dimension for MSM analysis

**Key Attributes**:
- `msm_key`: Surrogate key
- `msm_domain`: Media domain (e.g., cnn.com)
- `msm_name`: Media outlet name (e.g., CNN)
- `msm_category`: Media category (TV News, Print/Online, News Agency, European Media, Asian Media)
- `is_major_news`: Major news outlet flag
- `is_international`: International outlet flag
- `msm_hash`: Hash for natural key

### 7. dim_trend_type
**Purpose**: Trend type dimension for categorization analysis

**Key Attributes**:
- `trend_type_key`: Surrogate key
- `trend_type`: Trend type/genre
- `risk_type`: Risk/abuse type
- `trend_presentation_method`: Presentation method (video, image, narrative)
- `trend_type_hash`: Hash for natural key

## Bridge Tables

### 1. bridge_trend_content_tags
**Purpose**: Many-to-many relationship between trends and content tags/hashtags

**Key Attributes**:
- `trend_id`: Trend identifier
- `content_tag`: Normalized content tag
- `tag_category`: Tag type (hashtag, mention, identifier)
- `region`: Region context
- `created_at`: Trend creation timestamp

### 2. bridge_trend_google_products
**Purpose**: Many-to-many relationship between trends and potentially impacted Google products

**Key Attributes**:
- `trend_id`: Trend identifier
- `platform_key`: Google product identifier
- `product_name`: Google product name (denormalized)
- `region`: Region context
- `created_at`: Trend creation timestamp

### 3. bridge_trend_keywords
**Purpose**: Many-to-many relationship between trends and keywords for narrative analysis

**Key Attributes**:
- `trend_id`: Trend identifier
- `keyword`: Normalized keyword
- `region`: Region context
- `created_at`: Trend creation timestamp

### 4. bridge_trend_platform_spread
**Purpose**: Many-to-many relationship between trends and platforms they spread to

**Key Attributes**:
- `trend_id`: Trend identifier
- `platform_key`: Platform identifier
- `platform_name`: Platform name (denormalized)
- `region`: Region context
- `created_at`: Trend creation timestamp

### 5. bridge_trend_internal_tags
**Purpose**: Many-to-many relationship between trends and internal ActiveFence tags

**Key Attributes**:
- `trend_id`: Trend identifier
- `internal_tag`: Internal ActiveFence tag (normalized)
- `region`: Region context
- `created_at`: Trend creation timestamp
