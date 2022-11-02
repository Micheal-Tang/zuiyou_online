#! /usr/bin/env python
# -*-coding: utf-8 -*-
import pymongo

def data_pre():
    feature_zy = {
        "web_name": "post",
        "app_name": "zuiyou",
        "web_pack": [
            {
                "text": "帖子基础维度",
                "key": "post_type",
                "fields": [
                    {
                        "label": "起始日期",
                        "value": "create_start_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "create_end_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "发帖用户id",
                        "value": "omid",
                        "type": "string",
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0~5"},
                                {"label": "6~15s", "value": "6~15"},
                                {"label": "16~30s", "value": "16~30"},
                                {"label": "31~60s", "value": "31~60"},
                                {"label": "61~90s", "value": "61~90"},
                                {"label": "91~120s", "value": "91~120"},
                                {"label": "121~300s", "value": "121~300"},
                                {"label": "300s+", "value": "300+"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "帖子图片数量",
                        "value": "media_num",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "1", "value": "1"},
                                {"label": "2", "value": "2"},
                                {"label": "3", "value": "3"},
                                {"label": "4", "value": "4"},
                                {"label": "5", "value": "5"},
                                {"label": "6", "value": "6"},
                                {"label": "7", "value": "7"},
                                {"label": "8", "value": "8"},
                                {"label": "9", "value": "9"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "话题id",
                        "value": "tid",
                        "type": "string",
                    },
                    {
                        "label": "话题名称",
                        "value": "tname",
                        "type": "string",
                    },
                    {
                        "label": "帖子体裁",
                        "value": "ptype",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "文字", "value": "文字"},
                                {"label": "静图", "value": "静图"},
                                {"label": "长静图", "value": "长静图"},
                                {"label": "动图", "value": "动图"},
                                {"label": "视频", "value": "视频"},
                                {"label": "长文", "value": "长文"},
                                {"label": "音频", "value": "音频"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "一级分区名称",
                        "value": "l1part_name",
                        "type": "string",
                    },
                    {
                        "label": "二级分区名称",
                        "value": "l2part_name",
                        "type": "string",
                    },
                    {
                        "label": "一级分区id",
                        "value": "l1part_id",
                        "type": "string",
                    },
                    {
                        "label": "二级分区id",
                        "value": "l2part_id",
                        "type": "string",
                    },
                    {
                        "label": "内容长度",
                        "value": "content_length",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子状态",
                        "value": "status",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "自己删除", "value": "自己删除"},
                                {"label": "不可见", "value": "不可见"},
                                {"label": "自己可见", "value": "自己可见"},
                                {"label": "话题自见", "value": "话题自见"},
                                {"label": "可见", "value": "可见"},
                                {"label": "推荐", "value": "推荐"},
                                {"label": "话题推荐", "value": "话题推荐"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "年龄",
                        "value": "age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "16岁以下", "value": "16-"},
                                {"label": "16至18岁", "value": "16~18"},
                                {"label": "19至22岁", "value": "19~22"},
                                {"label": "22岁以上", "value": "22+"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "性别",
                        "value": "gender",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "女", "value": "女"},
                                {"label": "男", "value": "男"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "帖子消费维度",
                "key": "post_consume_type",
                "fields": [
                    {
                        "label": "性别",
                        "value": "gender",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "女", "value": "female"},
                                {"label": "男", "value": "male"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "操作系统",
                        "value": "os_type",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "Android", "value": "Android"},
                                {"label": "iOS", "value": "iOS"},
                            ],
                        },
                    },
                    {
                        "label": "年龄",
                        "value": "age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "16岁以下", "value": "16-"},
                                {"label": "16至18岁", "value": "16~18"},
                                {"label": "19至22岁", "value": "19~22"},
                                {"label": "22岁以上", "value": "22+"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "网络状态",
                        "value": "nt",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "4G", "value": "4g"},
                                {"label": "WiFi", "value": "wifi"},
                                {"label": "4G+WiFi", "value": "both"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "是否话题粉丝",
                        "value": "is_fan",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "是", "value": "1"},
                                {"label": "否", "value": "0"},
                            ],
                        },
                    },
                    {
                        "label": "设备右龄",
                        "value": "live_days_did",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "新用户", "value": "0"},
                                {"label": "1~7", "value": "1~7"},
                                {"label": "8~30", "value": "8~30"},
                                {"label": "30+", "value": "30+"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "是否推荐流",
                        "value": "is_feed",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "是", "value": "1"},
                                {"label": "否", "value": "0"},
                            ],
                        },
                    },
                    {
                        "label": "推荐流是否带神评",
                        "value": "pgod",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "是", "value": "1"},
                                {"label": "否", "value": "0"},
                            ],
                        },
                    },
                    {
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0", "value": "0"},
                                {"label": "1", "value": "1"},
                                {"label": "2", "value": "2"},
                                {"label": "3", "value": "3"},
                                {"label": "4~7", "value": "4~7"},
                                {"label": "8~14", "value": "8~14"},
                                {"label": "15~30", "value": "15~30"},
                                {"label": "30+", "value": "30+"},
                                {"label": "other", "value": "其他"},
                            ],
                        },
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "帖子消费指标",
                "key": "post_consume",
                "filterLen": 1,
                "fields": [
                    {
                        "label": "起始日期",
                        "value": "consume_start_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "consume_end_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "时间类型",
                        "value": "date_type",
                        "type": "select",
                        "rules": [
                            {"required": True, }
                        ],
                        "props": {
                            "options": [
                                {"label": "区间", "value": "period"},
                                {"label": "天", "value": "day"},
                            ],
                        },
                    },
                    {
                        "label": "刷贴量(请求)",
                        "value": "expose",
                        "type": "numbers",
                    },
                    {
                        "label": "刷贴量(真实曝光)",
                        "value": "real_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "消费量",
                        "value": "score",
                        "type": "numbers",
                    },
                    {
                        "label": "满意消费量",
                        "value": "score_satisfied",
                        "type": "numbers",
                    },
                    {
                        "label": "停留时长(min)",
                        "value": "stay_time",
                        "type": "numbers",
                    },
                    {
                        "label": "总播放视频次数",
                        "value": "total_play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "总播放视频时长(min)",
                        "value": "total_play_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴视频播放量",
                        "value": "play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴视频播放时长(min)",
                        "value": "play_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "评论视频播放量",
                        "value": "play_review_video",
                        "type": "numbers",
                    },
                    {
                        "label": "评论视频播放时长(min)",
                        "value": "play_review_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "播放左滑视频次数",
                        "value": "play_recommend_video",
                        "type": "numbers",
                    },
                    {
                        "label": "播放左滑视频时长(min)",
                        "value": "play_recommend_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "总浏览图片次数",
                        "value": "total_view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "总浏览图片时长(min)",
                        "value": "total_view_img_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴图片浏览量",
                        "value": "view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴图片浏览时长(min)",
                        "value": "view_img_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "详情次数",
                        "value": "detail_post",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子详情时长(min)",
                        "value": "detail_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "浏览帖子详情时长(min)",
                        "value": "view_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子点赞数",
                        "value": "like",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子点踩数",
                        "value": "dislike",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子分享数",
                        "value": "share",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子收藏数",
                        "value": "favor",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子举报数",
                        "value": "tedium",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论数",
                        "value": "create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论数",
                        "value": "reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论数",
                        "value": "total_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论点赞数",
                        "value": "like_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论点踩数",
                        "value": "dislike_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论分享数",
                        "value": "share_review",
                        "type": "numbers",
                    },
                    {
                        "label": "点击率",
                        "value": "ctr",
                        "type": "numbers",
                    },
                    {
                        "label": "满意ctr",
                        "value": "ctr_satisfied",
                        "type": "numbers",
                    },
                    {
                        "label": "单次曝光平均时长",
                        "value": "stay_time_avg",
                        "type": "numbers",
                    },
                    {
                        "label": "详情率",
                        "value": "detail_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "单次曝光详情时长",
                        "value": "detail_post_dur_avg",
                        "type": "numbers",
                    },
                    {
                        "label": "点赞率",
                        "value": "like_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "点踩率",
                        "value": "dislike_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "分享率",
                        "value": "share_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "收藏率",
                        "value": "favor_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论率",
                        "value": "create_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论率",
                        "value": "reply_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论率",
                        "value": "total_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "排序功能",
                "key": "order_func",
                "filterLen": 1,
                "fields": [
                    {
                        "label": "帖子排序",
                        "value": "rank",
                        "type": "numbers",
                    },
                    {
                        "label": "刷贴量(请求)",
                        "value": "expose",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "刷贴量(真实曝光)",
                        "value": "real_expose",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "消费量",
                        "value": "score",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "满意消费量",
                        "value": "score_satisfied",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "停留时长(min)",
                        "value": "stay_time",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总播放视频次数",
                        "value": "total_play_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总播放视频时长(min)",
                        "value": "total_play_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴视频播放量",
                        "value": "play_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴视频播放时长(min)",
                        "value": "play_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论视频播放量",
                        "value": "play_review_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论视频播放时长(min)",
                        "value": "play_review_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "播放左滑视频次数",
                        "value": "play_recommend_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "播放左滑视频时长(min)",
                        "value": "play_recommend_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总浏览图片次数",
                        "value": "total_view_img",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总浏览图片时长(min)",
                        "value": "total_view_img_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴图片浏览量",
                        "value": "view_img",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴图片浏览时长(min)",
                        "value": "view_img_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "详情次数",
                        "value": "detail_post",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子详情时长(min)",
                        "value": "detail_post_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "浏览帖子详情时长(min)",
                        "value": "view_post_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子点赞数",
                        "value": "like",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子点踩数",
                        "value": "dislike",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子分享数",
                        "value": "share",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子收藏数",
                        "value": "favor",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子举报数",
                        "value": "tedium",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "一级评论数",
                        "value": "create_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "二级评论数",
                        "value": "reply_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总评论数",
                        "value": "total_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论点赞数",
                        "value": "like_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论点踩数",
                        "value": "dislike_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论分享数",
                        "value": "share_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点击率",
                        "value": "ctr",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "满意ctr",
                        "value": "ctr_satisfied",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "单次曝光平均时长",
                        "value": "stay_time_avg",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "详情率",
                        "value": "detail_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "单次曝光详情时长",
                        "value": "detail_post_dur_avg",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点赞率",
                        "value": "like_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点踩率",
                        "value": "dislike_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "分享率",
                        "value": "share_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "收藏率",
                        "value": "favor_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "一级评论率",
                        "value": "create_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "二级评论率",
                        "value": "reply_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总评论率",
                        "value": "total_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                ],
            },
        ],
    }

    feature_pp = {
        "web_name": "post",
        "app_name": "zuiyou_lite",
        "web_pack": [
            {
                "text": "帖子基础维度",
                "key": "post_type",
                "fields": [
                    {
                        "label": "起始日期",
                        "value": "create_start_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "create_end_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "发帖用户id",
                        "value": "omid",
                        "type": "string",
                    },
                    {
                        "label": "帖子标签",
                        "value": "tag",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "美女", "value": "美女"},
                                {"label": "恐怖", "value": "恐怖"},
                                {"label": "普通ugc", "value": "普通ugc"},
                                {"label": "优质ugc", "value": "优质ugc"},
                                {"label": "恐怖提示", "value": "恐怖提示"},
                                {"label": "内涵", "value": "内涵"},
                                {"label": "重点话题帖", "value": "重点话题帖"},
                                {"label": "运营热帖", "value": "运营热帖"},
                                {"label": "102", "value": "102"},
                            ],
                        },
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0~5"},
                                {"label": "6~15s", "value": "6~15"},
                                {"label": "16~30s", "value": "16~30"},
                                {"label": "31~60s", "value": "31~60"},
                                {"label": "61~90s", "value": "61~90"},
                                {"label": "91~120s", "value": "91~120"},
                                {"label": "121~300s", "value": "121~300"},
                                {"label": "300s+", "value": "300+"},
                            ],
                        },
                    },
                    {
                        "label": "帖子图片数量",
                        "value": "media_num",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "1", "value": "1"},
                                {"label": "2", "value": "2"},
                                {"label": "3", "value": "3"},
                                {"label": "4", "value": "4"},
                                {"label": "5", "value": "5"},
                                {"label": "6", "value": "6"},
                                {"label": "7", "value": "7"},
                                {"label": "8", "value": "8"},
                                {"label": "9", "value": "9"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "话题id",
                        "value": "tid",
                        "type": "string",
                    },
                    {
                        "label": "话题名称",
                        "value": "tname",
                        "type": "string",
                    },
                    {
                        "label": "帖子体裁",
                        "value": "ptype",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "文字", "value": "文字"},
                                {"label": "静图", "value": "静图"},
                                {"label": "长静图", "value": "长静图"},
                                {"label": "动图", "value": "动图"},
                                {"label": "视频", "value": "视频"},
                                {"label": "长文", "value": "长文"},
                                {"label": "音频", "value": "音频"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "一级分区名称",
                        "value": "l1part_name",
                        "type": "string",
                    },
                    {
                        "label": "二级分区名称",
                        "value": "l2part_name",
                        "type": "string",
                    },
                    {
                        "label": "一级分区id",
                        "value": "l1part_id",
                        "type": "string",
                    },
                    {
                        "label": "二级分区id",
                        "value": "l2part_id",
                        "type": "string",
                    },
                    {
                        "label": "内容长度",
                        "value": "content_length",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子状态",
                        "value": "status",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "自己删除", "value": "自己删除"},
                                {"label": "不可见", "value": "不可见"},
                                {"label": "自己可见", "value": "自己可见"},
                                {"label": "话题自见", "value": "话题自见"},
                                {"label": "可见", "value": "可见"},
                                {"label": "推荐", "value": "推荐"},
                                {"label": "话题推荐", "value": "话题推荐"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "年龄",
                        "value": "age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "17岁及以下", "value": "17-"},
                                {"label": "18至22岁", "value": "18~22"},
                                {"label": "23至28岁", "value": "23~28"},
                                {"label": "28岁以上", "value": "28+"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "性别",
                        "value": "gender",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "女", "value": "女"},
                                {"label": "男", "value": "男"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "帖子消费维度",
                "key": "post_consume_type",
                "fields": [
                    {
                        "label": "性别",
                        "value": "gender",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "女", "value": "female"},
                                {"label": "男", "value": "male"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "操作系统",
                        "value": "os_type",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "Android", "value": "Android"},
                                {"label": "iOS", "value": "iOS"},
                            ],
                        },
                    },
                    {
                        "label": "年龄",
                        "value": "age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "17岁及以下", "value": "17-"},
                                {"label": "18至22岁", "value": "18~22"},
                                {"label": "23至28岁", "value": "23~28"},
                                {"label": "28岁以上", "value": "28+"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "网络状态",
                        "value": "nt",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "4G", "value": "4g"},
                                {"label": "WiFi", "value": "wifi"},
                                {"label": "4G+WiFi", "value": "both"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "是否话题粉丝",
                        "value": "is_fan",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "是", "value": "1"},
                                {"label": "否", "value": "0"},
                            ],
                        },
                    },
                    {
                        "label": "设备右龄",
                        "value": "live_days_did",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "新用户", "value": "0"},
                                {"label": "1~7", "value": "1~7"},
                                {"label": "8~30", "value": "8~30"},
                                {"label": "30+", "value": "30+"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "是否推荐流",
                        "value": "is_feed",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "是", "value": "1"},
                                {"label": "否", "value": "0"},
                            ],
                        },
                    },
                    {
                        "label": "推荐流是否带神评",
                        "value": "pgod",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "是", "value": "1"},
                                {"label": "否", "value": "0"},
                            ],
                        },
                    },
                    {
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0", "value": "0"},
                                {"label": "1", "value": "1"},
                                {"label": "2", "value": "2"},
                                {"label": "3", "value": "3"},
                                {"label": "4~7", "value": "4~7"},
                                {"label": "8~14", "value": "8~14"},
                                {"label": "15~30", "value": "15~30"},
                                {"label": "30+", "value": "30+"},
                                {"label": "other", "value": "其他"},
                            ],
                        },
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "帖子消费指标",
                "key": "post_consume",
                "filterLen": 1,
                "fields": [
                    {
                        "label": "起始日期",
                        "value": "consume_start_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "consume_end_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "时间类型",
                        "value": "date_type",
                        "type": "select",
                        "rules": [
                            {"required": True, }
                        ],
                        "props": {
                            "options": [
                                {"label": "区间", "value": "period"},
                                {"label": "天", "value": "day"},
                            ],
                        },
                    },
                    {
                        "label": "刷贴量(请求)",
                        "value": "expose",
                        "type": "numbers",
                    },
                    {
                        "label": "刷贴量(真实曝光)",
                        "value": "real_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "消费量",
                        "value": "score",
                        "type": "numbers",
                    },
                    {
                        "label": "满意消费量",
                        "value": "score_satisfied",
                        "type": "numbers",
                    },
                    {
                        "label": "停留时长(min)",
                        "value": "stay_time",
                        "type": "numbers",
                    },
                    {
                        "label": "总播放视频次数",
                        "value": "total_play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "总播放视频时长(min)",
                        "value": "total_play_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴视频播放量",
                        "value": "play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴视频播放时长(min)",
                        "value": "play_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "评论视频播放量",
                        "value": "play_review_video",
                        "type": "numbers",
                    },
                    {
                        "label": "评论视频播放时长(min)",
                        "value": "play_review_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "总浏览图片次数",
                        "value": "total_view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "总浏览图片时长(min)",
                        "value": "total_view_img_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴图片浏览量",
                        "value": "view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴图片浏览时长(min)",
                        "value": "view_img_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "详情次数",
                        "value": "detail_post",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子详情时长(min)",
                        "value": "detail_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "浏览帖子详情时长(min)",
                        "value": "view_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子点赞数",
                        "value": "like",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子点踩数",
                        "value": "dislike",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子分享数",
                        "value": "share",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子收藏数",
                        "value": "favor",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子举报数",
                        "value": "tedium",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论数",
                        "value": "create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论数",
                        "value": "reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论数",
                        "value": "total_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论点赞数",
                        "value": "like_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论点踩数",
                        "value": "dislike_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论分享数",
                        "value": "share_review",
                        "type": "numbers",
                    },
                    {
                        "label": "点击率",
                        "value": "ctr",
                        "type": "numbers",
                    },
                    {
                        "label": "满意ctr",
                        "value": "ctr_satisfied",
                        "type": "numbers",
                    },
                    {
                        "label": "单次曝光平均时长",
                        "value": "stay_time_avg",
                        "type": "numbers",
                    },
                    {
                        "label": "详情率",
                        "value": "detail_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "单次曝光详情时长",
                        "value": "detail_post_dur_avg",
                        "type": "numbers",
                    },
                    {
                        "label": "点赞率",
                        "value": "like_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "点踩率",
                        "value": "dislike_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "分享率",
                        "value": "share_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "收藏率",
                        "value": "favor_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论率",
                        "value": "create_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论率",
                        "value": "reply_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论率",
                        "value": "total_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "排序功能",
                "key": "order_func",
                "filterLen": 1,
                "fields": [
                    {
                        "label": "帖子排序",
                        "value": "rank",
                        "type": "numbers",
                    },
                    {
                        "label": "刷贴量(请求)",
                        "value": "expose",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "刷贴量(真实曝光)",
                        "value": "real_expose",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "消费量",
                        "value": "score",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "满意消费量",
                        "value": "score_satisfied",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "停留时长(min)",
                        "value": "stay_time",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总播放视频次数",
                        "value": "total_play_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总播放视频时长(min)",
                        "value": "total_play_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴视频播放量",
                        "value": "play_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴视频播放时长(min)",
                        "value": "play_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论视频播放量",
                        "value": "play_review_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论视频播放时长(min)",
                        "value": "play_review_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总浏览图片次数",
                        "value": "total_view_img",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总浏览图片时长(min)",
                        "value": "total_view_img_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴图片浏览量",
                        "value": "view_img",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴图片浏览时长(min)",
                        "value": "view_img_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "详情次数",
                        "value": "detail_post",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子详情时长(min)",
                        "value": "detail_post_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "浏览帖子详情时长(min)",
                        "value": "view_post_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子点赞数",
                        "value": "like",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子点踩数",
                        "value": "dislike",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子分享数",
                        "value": "share",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子收藏数",
                        "value": "favor",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子举报数",
                        "value": "tedium",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "一级评论数",
                        "value": "create_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "二级评论数",
                        "value": "reply_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总评论数",
                        "value": "total_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论点赞数",
                        "value": "like_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论点踩数",
                        "value": "dislike_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论分享数",
                        "value": "share_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点击率",
                        "value": "ctr",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "满意ctr",
                        "value": "ctr_satisfied",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "单次曝光平均时长",
                        "value": "stay_time_avg",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "详情率",
                        "value": "detail_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "单次曝光详情时长",
                        "value": "detail_post_dur_avg",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点赞率",
                        "value": "like_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点踩率",
                        "value": "dislike_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "分享率",
                        "value": "share_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "收藏率",
                        "value": "favor_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "一级评论率",
                        "value": "create_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "二级评论率",
                        "value": "reply_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总评论率",
                        "value": "total_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                ],
            },
        ],
    }

    feature_omg = {
        "web_name": "post",
        "app_name": "omg",
        "web_pack": [
            {
                "text": "帖子基础维度",
                "key": "post_type",
                "fields": [
                    {
                        "label": "起始日期",
                        "value": "create_start_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "create_end_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "发帖用户id",
                        "value": "omid",
                        "type": "string",
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0~5"},
                                {"label": "6~15s", "value": "6~15"},
                                {"label": "16~30s", "value": "16~30"},
                                {"label": "31~60s", "value": "31~60"},
                                {"label": "61~90s", "value": "61~90"},
                                {"label": "91~120s", "value": "91~120"},
                                {"label": "121~300s", "value": "121~300"},
                                {"label": "300s+", "value": "300+"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "帖子图片数量",
                        "value": "media_num",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "1", "value": "1"},
                                {"label": "2", "value": "2"},
                                {"label": "3", "value": "3"},
                                {"label": "4", "value": "4"},
                                {"label": "5", "value": "5"},
                                {"label": "6", "value": "6"},
                                {"label": "7", "value": "7"},
                                {"label": "8", "value": "8"},
                                {"label": "9", "value": "9"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "话题id",
                        "value": "tid",
                        "type": "string",
                    },
                    {
                        "label": "话题名称",
                        "value": "tname",
                        "type": "string",
                    },
                    {
                        "label": "帖子体裁",
                        "value": "ptype",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "文字", "value": "文字"},
                                {"label": "静图", "value": "静图"},
                                {"label": "长静图", "value": "长静图"},
                                {"label": "动图", "value": "动图"},
                                {"label": "视频", "value": "视频"},
                                {"label": "长文", "value": "长文"},
                                {"label": "音频", "value": "音频"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "一级分区名称",
                        "value": "l1part_name",
                        "type": "string",
                    },
                    {
                        "label": "二级分区名称",
                        "value": "l2part_name",
                        "type": "string",
                    },
                    {
                        "label": "一级分区id",
                        "value": "l1part_id",
                        "type": "string",
                    },
                    {
                        "label": "二级分区id",
                        "value": "l2part_id",
                        "type": "string",
                    },
                    {
                        "label": "内容长度",
                        "value": "content_length",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子状态",
                        "value": "status",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "自己删除", "value": "自己删除"},
                                {"label": "不可见", "value": "不可见"},
                                {"label": "自己可见", "value": "自己可见"},
                                {"label": "话题自见", "value": "话题自见"},
                                {"label": "可见", "value": "可见"},
                                {"label": "推荐", "value": "推荐"},
                                {"label": "话题推荐", "value": "话题推荐"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "年龄",
                        "value": "age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "18岁以下", "value": "18-"},
                                {"label": "18至24岁", "value": "18~24"},
                                {"label": "25至30岁", "value": "25~30"},
                                {"label": "30岁以上", "value": "30+"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "性别",
                        "value": "gender",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "女", "value": "女"},
                                {"label": "男", "value": "男"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "帖子消费维度",
                "key": "post_consume_type",
                "fields": [
                    {
                        "label": "性别",
                        "value": "gender",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "女", "value": "female"},
                                {"label": "男", "value": "male"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "操作系统",
                        "value": "os_type",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "Android", "value": "Android"},
                                {"label": "iOS", "value": "iOS"},
                            ],
                        },
                    },
                    {
                        "label": "年龄",
                        "value": "age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "18岁以下", "value": "18-"},
                                {"label": "18至24岁", "value": "18~24"},
                                {"label": "25至30岁", "value": "25~30"},
                                {"label": "30岁以上", "value": "30+"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "网络状态",
                        "value": "nt",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "4G", "value": "4g"},
                                {"label": "WiFi", "value": "wifi"},
                                {"label": "4G+WiFi", "value": "both"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "设备右龄",
                        "value": "live_days_did",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "新用户", "value": "0"},
                                {"label": "1~7", "value": "1~7"},
                                {"label": "8~30", "value": "8~30"},
                                {"label": "30+", "value": "30+"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "是否推荐流",
                        "value": "is_feed",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "是", "value": "1"},
                                {"label": "否", "value": "0"},
                            ],
                        },
                    },
                    {
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0", "value": "0"},
                                {"label": "1", "value": "1"},
                                {"label": "2", "value": "2"},
                                {"label": "3", "value": "3"},
                                {"label": "4~7", "value": "4~7"},
                                {"label": "8~14", "value": "8~14"},
                                {"label": "15~30", "value": "15~30"},
                                {"label": "30+", "value": "30+"},
                                {"label": "other", "value": "其他"},
                            ],
                        },
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "帖子消费指标",
                "key": "post_consume",
                "filterLen": 1,
                "fields": [
                    {
                        "label": "起始日期",
                        "value": "consume_start_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "consume_end_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "时间类型",
                        "value": "date_type",
                        "type": "select",
                        "rules": [
                            {"required": True, }
                        ],
                        "props": {
                            "options": [
                                {"label": "区间", "value": "period"},
                                {"label": "天", "value": "day"},
                            ],
                        },
                    },
                    {
                        "label": "刷贴量(请求)",
                        "value": "expose",
                        "type": "numbers",
                    },
                    {
                        "label": "刷贴量(真实曝光)",
                        "value": "real_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "消费量",
                        "value": "score",
                        "type": "numbers",
                    },
                    {
                        "label": "满意消费量",
                        "value": "score_satisfied",
                        "type": "numbers",
                    },
                    {
                        "label": "停留时长(min)",
                        "value": "stay_time",
                        "type": "numbers",
                    },
                    {
                        "label": "总播放视频次数",
                        "value": "total_play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "总播放视频时长(min)",
                        "value": "total_play_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴视频播放量",
                        "value": "play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴视频播放时长(min)",
                        "value": "play_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "评论视频播放量",
                        "value": "play_review_video",
                        "type": "numbers",
                    },
                    {
                        "label": "评论视频播放时长(min)",
                        "value": "play_review_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "播放左滑视频次数",
                        "value": "play_recommend_video",
                        "type": "numbers",
                    },
                    {
                        "label": "播放左滑视频时长(min)",
                        "value": "play_recommend_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "总浏览图片次数",
                        "value": "total_view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "总浏览图片时长(min)",
                        "value": "total_view_img_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴图片浏览量",
                        "value": "view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴图片浏览时长(min)",
                        "value": "view_img_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "详情次数",
                        "value": "detail_post",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子详情时长(min)",
                        "value": "detail_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "浏览帖子详情时长(min)",
                        "value": "view_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子点赞数",
                        "value": "like",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子点踩数",
                        "value": "dislike",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子分享数",
                        "value": "share",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子收藏数",
                        "value": "favor",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子举报数",
                        "value": "tedium",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论数",
                        "value": "create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论数",
                        "value": "reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论数",
                        "value": "total_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论点赞数",
                        "value": "like_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论点踩数",
                        "value": "dislike_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论分享数",
                        "value": "share_review",
                        "type": "numbers",
                    },
                    {
                        "label": "点击率",
                        "value": "ctr",
                        "type": "numbers",
                    },
                    {
                        "label": "满意ctr",
                        "value": "ctr_satisfied",
                        "type": "numbers",
                    },
                    {
                        "label": "单次曝光平均时长",
                        "value": "stay_time_avg",
                        "type": "numbers",
                    },
                    {
                        "label": "详情率",
                        "value": "detail_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "单次曝光详情时长",
                        "value": "detail_post_dur_avg",
                        "type": "numbers",
                    },
                    {
                        "label": "点赞率",
                        "value": "like_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "点踩率",
                        "value": "dislike_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "分享率",
                        "value": "share_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "收藏率",
                        "value": "favor_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论率",
                        "value": "create_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论率",
                        "value": "reply_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论率",
                        "value": "total_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "排序功能",
                "key": "order_func",
                "filterLen": 1,
                "fields": [
                    {
                        "label": "帖子排序",
                        "value": "rank",
                        "type": "numbers",
                    },
                    {
                        "label": "刷贴量(请求)",
                        "value": "expose",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "刷贴量(真实曝光)",
                        "value": "real_expose",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "消费量",
                        "value": "score",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "满意消费量",
                        "value": "score_satisfied",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "停留时长(min)",
                        "value": "stay_time",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总播放视频次数",
                        "value": "total_play_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总播放视频时长(min)",
                        "value": "total_play_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴视频播放量",
                        "value": "play_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴视频播放时长(min)",
                        "value": "play_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论视频播放量",
                        "value": "play_review_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论视频播放时长(min)",
                        "value": "play_review_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "播放左滑视频次数",
                        "value": "play_recommend_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "播放左滑视频时长(min)",
                        "value": "play_recommend_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总浏览图片次数",
                        "value": "total_view_img",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总浏览图片时长(min)",
                        "value": "total_view_img_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴图片浏览量",
                        "value": "view_img",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴图片浏览时长(min)",
                        "value": "view_img_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "详情次数",
                        "value": "detail_post",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子详情时长(min)",
                        "value": "detail_post_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "浏览帖子详情时长(min)",
                        "value": "view_post_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子点赞数",
                        "value": "like",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子点踩数",
                        "value": "dislike",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子分享数",
                        "value": "share",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子收藏数",
                        "value": "favor",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子举报数",
                        "value": "tedium",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "一级评论数",
                        "value": "create_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "二级评论数",
                        "value": "reply_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总评论数",
                        "value": "total_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论点赞数",
                        "value": "like_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论点踩数",
                        "value": "dislike_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论分享数",
                        "value": "share_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点击率",
                        "value": "ctr",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "满意ctr",
                        "value": "ctr_satisfied",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "单次曝光平均时长",
                        "value": "stay_time_avg",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "详情率",
                        "value": "detail_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "单次曝光详情时长",
                        "value": "detail_post_dur_avg",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点赞率",
                        "value": "like_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点踩率",
                        "value": "dislike_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "分享率",
                        "value": "share_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "收藏率",
                        "value": "favor_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "一级评论率",
                        "value": "create_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "二级评论率",
                        "value": "reply_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总评论率",
                        "value": "total_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                ],
            },
        ],
    }

    feature_maga = {
        "web_name": "post",
        "app_name": "maga",
        "web_pack": [
            {
                "text": "帖子基础维度",
                "key": "post_type",
                "fields": [
                    {
                        "label": "起始日期",
                        "value": "create_start_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "create_end_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "发帖用户id",
                        "value": "omid",
                        "type": "string",
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0~5"},
                                {"label": "6~15s", "value": "6~15"},
                                {"label": "16~30s", "value": "16~30"},
                                {"label": "31~60s", "value": "31~60"},
                                {"label": "61~90s", "value": "61~90"},
                                {"label": "91~120s", "value": "91~120"},
                                {"label": "121~300s", "value": "121~300"},
                                {"label": "300s+", "value": "300+"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "帖子图片数量",
                        "value": "media_num",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "1", "value": "1"},
                                {"label": "2", "value": "2"},
                                {"label": "3", "value": "3"},
                                {"label": "4", "value": "4"},
                                {"label": "5", "value": "5"},
                                {"label": "6", "value": "6"},
                                {"label": "7", "value": "7"},
                                {"label": "8", "value": "8"},
                                {"label": "9", "value": "9"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "话题id",
                        "value": "tid",
                        "type": "string",
                    },
                    {
                        "label": "话题名称",
                        "value": "tname",
                        "type": "string",
                    },
                    {
                        "label": "帖子体裁",
                        "value": "ptype",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "文字", "value": "文字"},
                                {"label": "静图", "value": "静图"},
                                {"label": "长静图", "value": "长静图"},
                                {"label": "动图", "value": "动图"},
                                {"label": "视频", "value": "视频"},
                                {"label": "长文", "value": "长文"},
                                {"label": "音频", "value": "音频"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "一级分区名称",
                        "value": "l1part_name",
                        "type": "string",
                    },
                    {
                        "label": "二级分区名称",
                        "value": "l2part_name",
                        "type": "string",
                    },
                    {
                        "label": "一级分区id",
                        "value": "l1part_id",
                        "type": "string",
                    },
                    {
                        "label": "二级分区id",
                        "value": "l2part_id",
                        "type": "string",
                    },
                    {
                        "label": "内容长度",
                        "value": "content_length",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子状态",
                        "value": "status",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "自己删除", "value": "自己删除"},
                                {"label": "不可见", "value": "不可见"},
                                {"label": "自己可见", "value": "自己可见"},
                                {"label": "话题自见", "value": "话题自见"},
                                {"label": "可见", "value": "可见"},
                                {"label": "推荐", "value": "推荐"},
                                {"label": "话题推荐", "value": "话题推荐"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "年龄",
                        "value": "age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "18岁及以下", "value": "18-"},
                                {"label": "19至24岁", "value": "19~24"},
                                {"label": "19至34岁", "value": "19~34"},
                                {"label": "25至34岁", "value": "25~34"},
                                {"label": "35至44岁", "value": "35~44"},
                                {"label": "35至60岁", "value": "35~60"},
                                {"label": "45至60岁", "value": "45~60"},
                                {"label": "60岁以上", "value": "60+"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "性别",
                        "value": "gender",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "女", "value": "女"},
                                {"label": "男", "value": "男"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "帖子消费维度",
                "key": "post_consume_type",
                "fields": [
                    {
                        "label": "性别",
                        "value": "gender",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "女", "value": "female"},
                                {"label": "男", "value": "male"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "操作系统",
                        "value": "os_type",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "Android", "value": "Android"},
                                {"label": "iOS", "value": "iOS"},
                            ],
                        },
                    },
                    {
                        "label": "年龄",
                        "value": "age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "18岁及以下", "value": "18-"},
                                {"label": "19至24岁", "value": "19~24"},
                                {"label": "19至34岁", "value": "19~34"},
                                {"label": "25至34岁", "value": "25~34"},
                                {"label": "35至44岁", "value": "35~44"},
                                {"label": "35至60岁", "value": "35~60"},
                                {"label": "45至60岁", "value": "45~60"},
                                {"label": "60岁以上", "value": "60+"},
                                {"label": "未知", "value": "未知"},
                            ],
                        },
                    },
                    {
                        "label": "网络状态",
                        "value": "nt",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "4G", "value": "4g"},
                                {"label": "WiFi", "value": "wifi"},
                                {"label": "4G+WiFi", "value": "both"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "设备右龄",
                        "value": "live_days_did",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "新用户", "value": "0"},
                                {"label": "1~7", "value": "1~7"},
                                {"label": "8~30", "value": "8~30"},
                                {"label": "30+", "value": "30+"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "是否推荐流",
                        "value": "is_feed",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "是", "value": "1"},
                                {"label": "否", "value": "0"},
                            ],
                        },
                    },
                    {
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0", "value": "0"},
                                {"label": "1", "value": "1"},
                                {"label": "2", "value": "2"},
                                {"label": "3", "value": "3"},
                                {"label": "4~7", "value": "4~7"},
                                {"label": "8~14", "value": "8~14"},
                                {"label": "15~30", "value": "15~30"},
                                {"label": "30+", "value": "30+"},
                                {"label": "other", "value": "其他"},
                            ],
                        },
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "帖子消费指标",
                "key": "post_consume",
                "filterLen": 1,
                "fields": [
                    {
                        "label": "起始日期",
                        "value": "consume_start_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "consume_end_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, },
                        ],
                    },
                    {
                        "label": "时间类型",
                        "value": "date_type",
                        "type": "select",
                        "rules": [
                            {"required": True, }
                        ],
                        "props": {
                            "options": [
                                {"label": "区间", "value": "period"},
                                {"label": "天", "value": "day"},
                            ],
                        },
                    },
                    {
                        "label": "刷贴量(请求)",
                        "value": "expose",
                        "type": "numbers",
                    },
                    {
                        "label": "刷贴量(真实曝光)",
                        "value": "real_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "消费量",
                        "value": "score",
                        "type": "numbers",
                    },
                    {
                        "label": "满意消费量",
                        "value": "score_satisfied",
                        "type": "numbers",
                    },
                    {
                        "label": "停留时长(min)",
                        "value": "stay_time",
                        "type": "numbers",
                    },
                    {
                        "label": "总播放视频次数",
                        "value": "total_play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "总播放视频时长(min)",
                        "value": "total_play_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴视频播放量",
                        "value": "play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴视频播放时长(min)",
                        "value": "play_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "评论视频播放量",
                        "value": "play_review_video",
                        "type": "numbers",
                    },
                    {
                        "label": "评论视频播放时长(min)",
                        "value": "play_review_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "播放左滑视频次数",
                        "value": "play_recommend_video",
                        "type": "numbers",
                    },
                    {
                        "label": "播放左滑视频时长(min)",
                        "value": "play_recommend_video_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "总浏览图片次数",
                        "value": "total_view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "总浏览图片时长(min)",
                        "value": "total_view_img_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴图片浏览量",
                        "value": "view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "主贴图片浏览时长(min)",
                        "value": "view_img_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "详情次数",
                        "value": "detail_post",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子详情时长(min)",
                        "value": "detail_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "浏览帖子详情时长(min)",
                        "value": "view_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子点赞数",
                        "value": "like",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子点踩数",
                        "value": "dislike",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子分享数",
                        "value": "share",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子收藏数",
                        "value": "favor",
                        "type": "numbers",
                    },
                    {
                        "label": "帖子举报数",
                        "value": "tedium",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论数",
                        "value": "create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论数",
                        "value": "reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论数",
                        "value": "total_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论点赞数",
                        "value": "like_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论点踩数",
                        "value": "dislike_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论分享数",
                        "value": "share_review",
                        "type": "numbers",
                    },
                    {
                        "label": "点击率",
                        "value": "ctr",
                        "type": "numbers",
                    },
                    {
                        "label": "满意ctr",
                        "value": "ctr_satisfied",
                        "type": "numbers",
                    },
                    {
                        "label": "单次曝光平均时长",
                        "value": "stay_time_avg",
                        "type": "numbers",
                    },
                    {
                        "label": "详情率",
                        "value": "detail_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "单次曝光详情时长",
                        "value": "detail_post_dur_avg",
                        "type": "numbers",
                    },
                    {
                        "label": "点赞率",
                        "value": "like_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "点踩率",
                        "value": "dislike_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "分享率",
                        "value": "share_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "收藏率",
                        "value": "favor_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论率",
                        "value": "create_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论率",
                        "value": "reply_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论率",
                        "value": "total_review_rate",
                        "type": "numbers",
                    },
                    {
                        "label": "条件关系",
                        "value": "relation",
                        "type": "selectString",
                    },
                ],
            },
            {
                "text": "排序功能",
                "key": "order_func",
                "filterLen": 1,
                "fields": [
                    {
                        "label": "帖子排序",
                        "value": "rank",
                        "type": "numbers",
                    },
                    {
                        "label": "刷贴量(请求)",
                        "value": "expose",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "刷贴量(真实曝光)",
                        "value": "real_expose",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "消费量",
                        "value": "score",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "满意消费量",
                        "value": "score_satisfied",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "停留时长(min)",
                        "value": "stay_time",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总播放视频次数",
                        "value": "total_play_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总播放视频时长(min)",
                        "value": "total_play_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴视频播放量",
                        "value": "play_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴视频播放时长(min)",
                        "value": "play_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论视频播放量",
                        "value": "play_review_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论视频播放时长(min)",
                        "value": "play_review_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "播放左滑视频次数",
                        "value": "play_recommend_video",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "播放左滑视频时长(min)",
                        "value": "play_recommend_video_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总浏览图片次数",
                        "value": "total_view_img",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总浏览图片时长(min)",
                        "value": "total_view_img_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴图片浏览量",
                        "value": "view_img",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "主贴图片浏览时长(min)",
                        "value": "view_img_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "详情次数",
                        "value": "detail_post",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子详情时长(min)",
                        "value": "detail_post_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "浏览帖子详情时长(min)",
                        "value": "view_post_dur",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子点赞数",
                        "value": "like",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子点踩数",
                        "value": "dislike",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子分享数",
                        "value": "share",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子收藏数",
                        "value": "favor",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "帖子举报数",
                        "value": "tedium",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "一级评论数",
                        "value": "create_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "二级评论数",
                        "value": "reply_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总评论数",
                        "value": "total_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论点赞数",
                        "value": "like_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论点踩数",
                        "value": "dislike_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "评论分享数",
                        "value": "share_review",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点击率",
                        "value": "ctr",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "满意ctr",
                        "value": "ctr_satisfied",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "单次曝光平均时长",
                        "value": "stay_time_avg",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "详情率",
                        "value": "detail_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "单次曝光详情时长",
                        "value": "detail_post_dur_avg",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点赞率",
                        "value": "like_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "点踩率",
                        "value": "dislike_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "分享率",
                        "value": "share_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "收藏率",
                        "value": "favor_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "一级评论率",
                        "value": "create_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "二级评论率",
                        "value": "reply_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                    {
                        "label": "总评论率",
                        "value": "total_review_rate",
                        "type": "select",
                        "props": {
                            "options": [
                                {"label": "正序", "value": "asc"},
                                {"label": "倒序", "value": "desc"},
                            ],
                        },
                    },
                ],
            },
        ],
    }

    return feature_zy, feature_pp, feature_omg, feature_maga

def connect():
    host = "xxx"
    db = "xxx"
    table = "xxx"

    client = pymongo.MongoClient(host= host)
    mongodb = client[db]
    collection = mongodb[table]
    return collection

def insert(collection,values):
    collection.insert_one(values)

def delete(collection,webname):
    collection.remove({'web_name':str(webname)})

if __name__ == '__main__':
    db = connect()
    value_zy, value_pp, value_omg, value_maga = data_pre()
    delete(db,'post')
    insert(db,value_zy)
    insert(db,value_pp)
    insert(db,value_omg)
    insert(db,value_maga)
