import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

export interface Article {
  id: string;
  title: string;
  summary: string;
  url: string;
  source_domain: string;
  published_at: string;
  country_code: string;
  tone?: number;
  image_url?: string;
}

export interface ArticleDetail {
  article: RawArticle;
  llm_content: string;
  cached: boolean;
}

// 后端返回的原始数据结构
interface RawArticle {
  id: number;
  gkg_record_id: string;
  country_code: string;
  source: string;
  url: string;
  date_added: number;
  title?: string;
  authors?: string[];
  tone?: number;
  images?: string[];
  themes?: string[];
  persons?: string[];
  organizations?: string[];
  locations?: string[];
  quotations?: string[];
}

// 将 date_added (YYYYMMDDHHMMSS) 转换为 ISO 日期字符串
function formatDateAdded(dateInt: number): string {
  const str = String(dateInt);
  if (str.length !== 14) return '';
  const year = str.slice(0, 4);
  const month = str.slice(4, 6);
  const day = str.slice(6, 8);
  const hour = str.slice(8, 10);
  const min = str.slice(10, 12);
  return `${year}-${month}-${day}T${hour}:${min}:00`;
}

// 从 URL 提取域名作为标题（fallback）
function extractTitle(raw: RawArticle): string {
  // 优先使用后端返回的 title
  if (raw.title && raw.title.trim()) {
    return raw.title.trim();
  }
  // fallback: 使用 themes
  if (raw.themes && raw.themes.length > 0) {
    return raw.themes.slice(0, 3).join(' | ');
  }
  // fallback: 使用域名
  try {
    const hostname = new URL(raw.url).hostname;
    return hostname.replace('www.', '');
  } catch {
    return 'News Article';
  }
}

// 映射后端数据到前端格式
function mapArticle(raw: RawArticle): Article {
  return {
    id: String(raw.id),
    title: extractTitle(raw),
    summary: raw.quotations?.join(' ') || raw.themes?.slice(0, 5).join(', ') || '',
    url: raw.url,
    source_domain: raw.source || '',
    published_at: formatDateAdded(raw.date_added),
    country_code: raw.country_code,
    tone: raw.tone,
    image_url: raw.images?.[0]
  };
}

export const articlesApi = {
  async getArticles(countryCode: string, page: number = 1, pageSize: number = 20): Promise<{ articles: Article[], total: number, totalPages: number }> {
    const response = await axios.get(`${API_BASE_URL}/api/articles/`, {
      params: { country_code: countryCode, days: 1, page, page_size: pageSize }
    });
    const data = response.data.data;
    const rawArticles: RawArticle[] = data?.articles || [];
    return {
      articles: rawArticles.map(mapArticle),
      total: data?.pagination?.total || 0,
      totalPages: data?.pagination?.total_pages || 0
    };
  },

  async getArticleDetail(articleId: string, language: string = 'zh'): Promise<ArticleDetail> {
    const response = await axios.get(`${API_BASE_URL}/api/articles/${articleId}/detail`, {
      params: { language }
    });
    return response.data.data;
  }
};
