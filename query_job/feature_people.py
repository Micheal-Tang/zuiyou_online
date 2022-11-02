#! /usr/bin/env python
# -*-coding: utf-8 -*-
import pymongo

def data_pre():
    feature_zy = {
        "web_name" : "user",
        "app_name" : "zuiyou",
        "web_pack" : [
            {
                "text" : "用户条件",
                "key" : "user",
                "fields" : [
                    {
                        "label": "时间类型",
                        "value": "user_date_type",
                        "type": "select",
                        "rules":[
                            {"required": True,}
                        ],
                        "props": {
                            "options": [
                                {"label": "区间", "value": "period"},
                                {"label": "天", "value": "day"},
                            ],
                        },
                    },
                    {
                        "label": "起始日期",
                        "value": "user_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "user_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "实验id",
                        "value": "exp_id",
                        "type": "numbers",
                    },
                    {
                        "label": "实验分组",
                        "value": "exp_group",
                        "type": "numbers",
                    },
                    {
                        "label": "设备过去7天活跃天数",
                        "value": "lt7_did",
                        "type": "numbers",
                    },
                    {
                        "label": "设备过去30天活跃天数",
                        "value": "lt30_did",
                        "type": "numbers",
                    },
                    {
                        "label" : "mid右龄",
                        "value" : "live_days_mid",
                        "type" : "numbers",
                    },
                    {
                        "label": "did右龄",
                        "value": "live_days_did",
                        "type": "numbers",
                    },
                    {
                        "label": "mid几天前登陆",
                        "value": "last_days_mid",
                        "type": "numbers",
                    },
                    {
                        "label": "did几天前登陆",
                        "value": "last_days_did",
                        "type": "numbers",
                    },
                    {
                        "label": "首次登陆方式",
                        "value": "first_launch_type",
                        "type": "select",
                        "props" : {
                            "mode": "multiple",
                            "options": [
                                {"label": "H5","value":"h5"},
                                {"label": "自然启动", "value": "natural"},
                                {"label": "聊天信息", "value": "push-chat"},
                                {"label": "私信", "value": "push-msg"},
                                {"label": "帖子信息", "value": "push-post"},
                                {"label": "红点", "value": "red_icon"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "注册日期",
                        "value": "reg_date",
                        "type": "datetime",
                    },
                    {
                        "label": "网络状态",
                        "value": "nt",
                        "type": "select",
                        "props" : {
                            "mode": "multiple",
                            "options": [
                                {"label": "4G","value":"4g"},
                                {"label": "WiFi", "value": "wifi"},
                                {"label": "4G+WiFi", "value": "both"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "地区",
                        "value": "province",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "上海市", "value": "上海市"},
                                {"label": "云南省", "value": "云南省"},
                                {"label": "内蒙古", "value": "内蒙古"},
                                {"label": "北京市", "value": "北京市"},
                                {"label": "台湾省", "value": "台湾省"},
                                {"label": "吉林省", "value": "吉林省"},
                                {"label": "四川省", "value": "四川省"},
                                {"label": "天津市", "value": "天津市"},
                                {"label": "宁夏", "value": "宁夏"},
                                {"label": "安徽省", "value": "安徽省"},
                                {"label": "山东省", "value": "山东省"},
                                {"label": "山西省", "value": "山西省"},
                                {"label": "广东省", "value": "广东省"},
                                {"label": "广西", "value": "广西"},
                                {"label": "新疆", "value": "新疆"},
                                {"label": "江苏省", "value": "江苏省"},
                                {"label": "江西省", "value": "江西省"},
                                {"label": "河北省", "value": "河北省"},
                                {"label": "河南省", "value": "河南省"},
                                {"label": "浙江省", "value": "浙江省"},
                                {"label": "海南省", "value": "海南省"},
                                {"label": "湖北省", "value": "湖北省"},
                                {"label": "湖南省", "value": "湖南省"},
                                {"label": "澳门", "value": "澳门"},
                                {"label": "甘肃省", "value": "甘肃省"},
                                {"label": "福建省", "value": "福建省"},
                                {"label": "西藏", "value": "西藏"},
                                {"label": "贵州省", "value": "贵州省"},
                                {"label": "辽宁省", "value": "辽宁省"},
                                {"label": "重庆市", "value": "重庆市"},
                                {"label": "陕西省", "value": "陕西省"},
                                {"label": "青海省", "value": "青海省"},
                                {"label": "香港", "value": "香港"},
                                {"label": "黑龙江省", "value": "黑龙江省"},
                            ],
                        },
                    },
                    {
                        "label": "版本",
                        "value": "app_version",
                        "type": "string",
                    },
                    {
                        "label": "操作系统",
                        "value": "os_type",
                        "type": "select",
                        "props" : {
                            "mode": "multiple",
                            "options": [
                                {"label": "Android","value":"Android"},
                                {"label": "iOS", "value": "iOS"},
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
                                {"label": "女", "value": "female"},
                                {"label": "男", "value": "male"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "渠道",
                        "value": "channel_click",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "华为", "value": "huawei"},
                                {"label": "OPPO", "value": "oppo"},
                                {"label": "vivo", "value": "vivo"},
                                {"label": "小米", "value": "xiaomi"},
                                {"label": "苹果商店", "value": "appstore"},
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
                                {"label": "16至18岁", "value": "16-18"},
                                {"label": "19至22岁", "value": "19-22"},
                                {"label": "22岁以上", "value": "22+"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "时长(min)",
                        "value": "duration",
                        "type": "numbers",
                    },
                    {
                        "label": "启动次数",
                        "value": "session_launch",
                        "type": "numbers",
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
                        "label": "图片贴-刷帖量",
                        "value": "img_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "图片贴-消费量",
                        "value": "img_score",
                        "type": "numbers",
                    },
                    {
                        "label": "视频贴-刷帖量",
                        "value": "video_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "视频贴-消费量",
                        "value": "video_score",
                        "type": "numbers",
                    },
                    {
                        "label": "进详情次数",
                        "value": "detail_post",
                        "type": "numbers",
                    },
                    {
                        "label": "进详情时长(min)",
                        "value": "view_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "创建帖子数",
                        "value": "create_post",
                        "type": "numbers",
                    },
                    {
                        "label": "点赞帖子次数",
                        "value": "like_post",
                        "type": "numbers",
                    },
                    {
                        "label": "点踩帖子次数",
                        "value": "dislike_post",
                        "type": "numbers",
                    },
                    {
                        "label": "分享帖子次数",
                        "value": "share_post",
                        "type": "numbers",
                    },
                    {
                        "label": "收藏帖子次数",
                        "value": "favor_post",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论量",
                        "value": "create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论量",
                        "value": "reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论量",
                        "value": "total_review",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论点赞量",
                        "value": "like_create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论点赞量",
                        "value": "like_reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论总点赞量",
                        "value": "like_review",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论点踩量",
                        "value": "dislike_create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论点踩量",
                        "value": "dislike_reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论总点踩量",
                        "value": "dislike_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论分享量",
                        "value": "share_review",
                        "type": "numbers",
                    },
                    {
                        "label": "创建弹幕量",
                        "value": "create_danmaku",
                        "type": "numbers",
                    },
                    {
                        "label": "弹幕点赞量",
                        "value": "like_danmaku",
                        "type": "numbers",
                    },
                    {
                        "label": "弹幕点踩量",
                        "value": "dislike_danmaku",
                        "type": "numbers",
                    },
                    {
                        "label": "总图片浏览量",
                        "value": "total_view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "总图片浏览时长(min)",
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
                        "label": "总视频播放量",
                        "value": "total_play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "总视频播放时长(min)",
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
                ],
            },
            {
                "text" : "帖子消费条件",
                "key" : "post_consume",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "post_consume_date_type",
                        "type": "select",
                        "rules": [
                            {"required": True,}
                        ],
                        "props": {
                            "options": [
                                {"label": "区间", "value": "period"},
                                {"label": "天", "value": "day"},
                            ],
                        },
                    },
                    {
                        "label": "起始日期",
                        "value": "post_consume_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "post_consume_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0-5"},
                                {"label": "6~15s", "value": "6-15"},
                                {"label": "16~30s", "value": "16-30"},
                                {"label": "31~60s", "value": "31-60"},
                                {"label": "61~90s", "value": "61-90"},
                                {"label": "91~120s", "value": "91-120"},
                                {"label": "121~300s", "value": "121-300"},
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
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0天", "value": "0"},
                                {"label": "1天", "value": "1"},
                                {"label": "2天", "value": "2"},
                                {"label": "3天", "value": "3"},
                                {"label": "4~7天", "value": "4-7"},
                                {"label": "8~14天", "value": "8-14"},
                                {"label": "15~30天", "value": "15-30"},
                                {"label": "30天+", "value": "30+"},
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
                ],
            },
            {
                "text" : "埋点行为",
                "key" : "actionlog_consume",
                "fields" : [
                    {
                        "label": "时间类型",
                        "value": "actionlog_consume_date_type",
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
                        "label": "起始日期",
                        "value": "actionlog_consume_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "actionlog_consume_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label" : "type",
                        "value" : "type",
                        "type" : "string",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "stype",
                        "value": "stype",
                        "type": "string",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "frominfo",
                        "value": "frominfo",
                        "type": "string",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "pv",
                        "value": "pv",
                        "type": "numbers",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                ],
            },
            {
                "text": "话题消费",
                "key": "topic_consume",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "topic_consume_date_type",
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
                        "label": "起始日期",
                        "value": "topic_consume_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "topic_consume_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
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
                        "label": "总进话题详情次数",
                        "value": "total_detail_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "总进话题详情时长(min)",
                        "value": "total_detail_topic_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "进话题热门流次数",
                        "value": "hot_detail_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "进话题热门流时长(min)",
                        "value": "hot_detail_topic_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "进话题新帖流次数",
                        "value": "new_detail_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "进话题新帖流时长(min)",
                        "value": "new_detail_topic_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内曝光",
                        "value": "expose_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内真实曝光",
                        "value": "real_expose_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内消费",
                        "value": "score_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内进详情",
                        "value": "detail_post_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内创建帖子",
                        "value": "create_post_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内帖子点赞",
                        "value": "like_post_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内分享帖子",
                        "value": "share_post_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内收藏帖子",
                        "value": "favor_post_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内创建评论量",
                        "value": "create_review_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内回复评论量",
                        "value": "reply_review_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内总评论量",
                        "value": "total_review_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内总点赞评论量",
                        "value": "like_review_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内点赞一级评论量",
                        "value": "like_create_review_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内点赞二级评论量",
                        "value": "like_reply_review_topic",
                        "type": "numbers",
                    },
                ],
            },
            {
                "text" : "发帖行为",
                "key" : "send_post",
                "fields" : [
                    {
                        "label": "时间类型",
                        "value": "send_post_date_type",
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
                        "label": "起始日期",
                        "value": "send_post_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "send_post_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0-5"},
                                {"label": "6~15s", "value": "6-15"},
                                {"label": "16~30s", "value": "16-30"},
                                {"label": "31~60s", "value": "31-60"},
                                {"label": "61~90s", "value": "61-90"},
                                {"label": "91~120s", "value": "91-120"},
                                {"label": "121~300s", "value": "121-300"},
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
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0天", "value": "0"},
                                {"label": "1天", "value": "1"},
                                {"label": "2天", "value": "2"},
                                {"label": "3天", "value": "3"},
                                {"label": "4~7天", "value": "4-7"},
                                {"label": "8~14天", "value": "8-14"},
                                {"label": "15~30天", "value": "15-30"},
                                {"label": "30天+", "value": "30+"},
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
                        "label": "发帖量",
                        "value": "post",
                        "type": "numbers",
                    },
                ],
            },
            {
                "text" : "发评论行为",
                "key" : "send_comment",
                "fields" : [
                    {
                        "label": "时间类型",
                        "value": "send_comment_date_type",
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
                        "label": "起始日期",
                        "value": "send_comment_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "send_comment_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "评论体裁",
                        "value": "rtype",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "文字", "value": "文字"},
                                {"label": "语音", "value": "语音"},
                                {"label": "图片", "value": "图片"},
                                {"label": "视频", "value": "视频"},
                            ],
                        },
                    },
                    {
                        "label": "评论类型",
                        "value": "ctype",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "一级评论", "value": "一级评论"},
                                {"label": "二级评论", "value": "二级评论"},
                            ],
                        },
                    },
                    {
                        "label": "评论量",
                        "value": "review",
                        "type": "numbers",
                    },
                ],
            },
            {
                "text": "关注行为",
                "key": "attention",
                "fields": [
                    {
                        "label": "关注话题起始日期",
                        "value": "attention_tid_start_date",
                        "type": "datetime",
                    },
                    {
                        "label": "关注话题结束日期",
                        "value": "attention_tid_end_date",
                        "type": "datetime",
                    },
                    {
                        "label": "关注话题id",
                        "value": "attention_tid",
                        "type": "string",
                    },
                    {
                        "label": "关注用户起始日期",
                        "value": "attention_mid_start_date",
                        "type": "datetime",
                    },
                    {
                        "label": "关注用户结束日期",
                        "value": "attention_mid_end_date",
                        "type": "datetime",
                    },
                    {
                        "label": "关注用户id",
                        "value": "attention_mid",
                        "type": "string",
                    },
                ],
            },
        ],
    }

    feature_pp = {
        "web_name": "user",
        "app_name": "zuiyou_lite",
        "web_pack": [
            {
                "text": "用户条件",
                "key": "user",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "user_date_type",
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
                        "label": "起始日期",
                        "value": "user_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "user_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "实验id",
                        "value": "exp_id",
                        "type": "numbers",
                    },
                    {
                        "label": "实验分组",
                        "value": "exp_group",
                        "type": "numbers",
                    },
                    {
                        "label": "设备过去7天活跃天数",
                        "value": "lt7_did",
                        "type": "numbers",
                    },
                    {
                        "label": "设备过去30天活跃天数",
                        "value": "lt30_did",
                        "type": "numbers",
                    },
                    {
                        "label": "mid右龄",
                        "value": "live_days_mid",
                        "type": "numbers",
                    },
                    {
                        "label": "did右龄",
                        "value": "live_days_did",
                        "type": "numbers",
                    },
                    {
                        "label": "mid几天前登陆",
                        "value": "last_days_mid",
                        "type": "numbers",
                    },
                    {
                        "label": "did几天前登陆",
                        "value": "last_days_did",
                        "type": "numbers",
                    },
                    {
                        "label": "首次登陆方式",
                        "value": "first_launch_type",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "H5", "value": "h5"},
                                {"label": "自然启动", "value": "natural"},
                                {"label": "聊天信息", "value": "push-chat"},
                                {"label": "私信", "value": "push-msg"},
                                {"label": "帖子信息", "value": "push-post"},
                                {"label": "红点", "value": "red_icon"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "注册日期",
                        "value": "reg_date",
                        "type": "datetime",
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
                        "label": "地区",
                        "value": "province",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "上海市", "value": "上海市"},
                                {"label": "云南省", "value": "云南省"},
                                {"label": "内蒙古", "value": "内蒙古"},
                                {"label": "北京市", "value": "北京市"},
                                {"label": "台湾省", "value": "台湾省"},
                                {"label": "吉林省", "value": "吉林省"},
                                {"label": "四川省", "value": "四川省"},
                                {"label": "天津市", "value": "天津市"},
                                {"label": "宁夏", "value": "宁夏"},
                                {"label": "安徽省", "value": "安徽省"},
                                {"label": "山东省", "value": "山东省"},
                                {"label": "山西省", "value": "山西省"},
                                {"label": "广东省", "value": "广东省"},
                                {"label": "广西", "value": "广西"},
                                {"label": "新疆", "value": "新疆"},
                                {"label": "江苏省", "value": "江苏省"},
                                {"label": "江西省", "value": "江西省"},
                                {"label": "河北省", "value": "河北省"},
                                {"label": "河南省", "value": "河南省"},
                                {"label": "浙江省", "value": "浙江省"},
                                {"label": "海南省", "value": "海南省"},
                                {"label": "湖北省", "value": "湖北省"},
                                {"label": "湖南省", "value": "湖南省"},
                                {"label": "澳门", "value": "澳门"},
                                {"label": "甘肃省", "value": "甘肃省"},
                                {"label": "福建省", "value": "福建省"},
                                {"label": "西藏", "value": "西藏"},
                                {"label": "贵州省", "value": "贵州省"},
                                {"label": "辽宁省", "value": "辽宁省"},
                                {"label": "重庆市", "value": "重庆市"},
                                {"label": "陕西省", "value": "陕西省"},
                                {"label": "青海省", "value": "青海省"},
                                {"label": "香港", "value": "香港"},
                                {"label": "黑龙江省", "value": "黑龙江省"},
                            ],
                        },
                    },
                    {
                        "label": "版本",
                        "value": "app_version",
                        "type": "string",
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
                        "label": "渠道",
                        "value": "channel_click",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "华为", "value": "huawei"},
                                {"label": "OPPO", "value": "oppo"},
                                {"label": "vivo", "value": "vivo"},
                                {"label": "小米", "value": "xiaomi"},
                                {"label": "苹果商店", "value": "appstore"},
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
                                {"label": "18至22岁", "value": "18-22"},
                                {"label": "23至28岁", "value": "23-28"},
                                {"label": "28岁以上", "value": "28+"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "时长(min)",
                        "value": "duration",
                        "type": "numbers",
                    },
                    {
                        "label": "启动次数",
                        "value": "session_launch",
                        "type": "numbers",
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
                        "label": "图片贴-刷帖量",
                        "value": "img_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "图片贴-消费量",
                        "value": "img_score",
                        "type": "numbers",
                    },
                    {
                        "label": "视频贴-刷帖量",
                        "value": "video_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "视频贴-消费量",
                        "value": "video_score",
                        "type": "numbers",
                    },
                    {
                        "label": "进详情次数",
                        "value": "detail_post",
                        "type": "numbers",
                    },
                    {
                        "label": "进详情时长(min)",
                        "value": "view_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "创建帖子数",
                        "value": "create_post",
                        "type": "numbers",
                    },
                    {
                        "label": "点赞帖子次数",
                        "value": "like_post",
                        "type": "numbers",
                    },
                    {
                        "label": "点踩帖子次数",
                        "value": "dislike_post",
                        "type": "numbers",
                    },
                    {
                        "label": "分享帖子次数",
                        "value": "share_post",
                        "type": "numbers",
                    },
                    {
                        "label": "收藏帖子次数",
                        "value": "favor_post",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论量",
                        "value": "create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论量",
                        "value": "reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论量",
                        "value": "total_review",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论点赞量",
                        "value": "like_create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论点赞量",
                        "value": "like_reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论总点赞量",
                        "value": "like_review",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论点踩量",
                        "value": "dislike_create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论点踩量",
                        "value": "dislike_reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论总点踩量",
                        "value": "dislike_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论分享量",
                        "value": "share_review",
                        "type": "numbers",
                    },
                    {
                        "label": "总图片浏览量",
                        "value": "total_view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "总图片浏览时长(min)",
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
                        "label": "总视频播放量",
                        "value": "total_play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "总视频播放时长(min)",
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
                ],
            },
            {
                "text": "帖子消费条件",
                "key": "post_consume",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "post_consume_date_type",
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
                        "label": "起始日期",
                        "value": "post_consume_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "post_consume_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0-5"},
                                {"label": "6~15s", "value": "6-15"},
                                {"label": "16~30s", "value": "16-30"},
                                {"label": "31~60s", "value": "31-60"},
                                {"label": "61~90s", "value": "61-90"},
                                {"label": "91~120s", "value": "91-120"},
                                {"label": "121~300s", "value": "121-300"},
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
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0天", "value": "0"},
                                {"label": "1天", "value": "1"},
                                {"label": "2天", "value": "2"},
                                {"label": "3天", "value": "3"},
                                {"label": "4~7天", "value": "4-7"},
                                {"label": "8~14天", "value": "8-14"},
                                {"label": "15~30天", "value": "15-30"},
                                {"label": "30天+", "value": "30+"},
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
                ],
            },
            {
                "text": "埋点行为",
                "key": "actionlog_consume",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "actionlog_consume_date_type",
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
                        "label": "起始日期",
                        "value": "actionlog_consume_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "actionlog_consume_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "type",
                        "value": "type",
                        "type": "string",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "stype",
                        "value": "stype",
                        "type": "string",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "frominfo",
                        "value": "frominfo",
                        "type": "string",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "pv",
                        "value": "pv",
                        "type": "numbers",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                ],
            },
            {
                "text": "话题消费",
                "key": "topic_consume",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "topic_consume_date_type",
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
                        "label": "起始日期",
                        "value": "topic_consume_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "topic_consume_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
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
                        "label": "总进话题详情次数",
                        "value": "total_detail_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "总进话题详情时长(min)",
                        "value": "total_detail_topic_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "进话题热门流次数",
                        "value": "hot_detail_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "进话题热门流时长(min)",
                        "value": "hot_detail_topic_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "进话题新帖流次数",
                        "value": "new_detail_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "进话题新帖流时长(min)",
                        "value": "new_detail_topic_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内曝光",
                        "value": "expose_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内真实曝光",
                        "value": "real_expose_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内消费",
                        "value": "score_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内进详情",
                        "value": "detail_post_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内创建帖子",
                        "value": "create_post_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内帖子点赞",
                        "value": "like_post_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内分享帖子",
                        "value": "share_post_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内收藏帖子",
                        "value": "favor_post_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内创建评论量",
                        "value": "create_review_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内回复评论量",
                        "value": "reply_review_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内总评论量",
                        "value": "total_review_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内总点赞评论量",
                        "value": "like_review_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内点赞一级评论量",
                        "value": "like_create_review_topic",
                        "type": "numbers",
                    },
                    {
                        "label": "话题内点赞二级评论量",
                        "value": "like_reply_review_topic",
                        "type": "numbers",
                    },
                ],
            },
            {
                "text": "发帖行为",
                "key": "send_post",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "send_post_date_type",
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
                        "label": "起始日期",
                        "value": "send_post_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "send_post_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0-5"},
                                {"label": "6~15s", "value": "6-15"},
                                {"label": "16~30s", "value": "16-30"},
                                {"label": "31~60s", "value": "31-60"},
                                {"label": "61~90s", "value": "61-90"},
                                {"label": "91~120s", "value": "91-120"},
                                {"label": "121~300s", "value": "121-300"},
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
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0天", "value": "0"},
                                {"label": "1天", "value": "1"},
                                {"label": "2天", "value": "2"},
                                {"label": "3天", "value": "3"},
                                {"label": "4~7天", "value": "4-7"},
                                {"label": "8~14天", "value": "8-14"},
                                {"label": "15~30天", "value": "15-30"},
                                {"label": "30天+", "value": "30+"},
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
                        "label": "发帖量",
                        "value": "post",
                        "type": "numbers",
                    },
                ],
            },
            {
                "text": "发评论行为",
                "key": "send_comment",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "send_comment_date_type",
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
                        "label": "起始日期",
                        "value": "send_comment_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "send_comment_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "评论体裁",
                        "value": "rtype",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "文字", "value": "文字"},
                                {"label": "语音", "value": "语音"},
                                {"label": "图片", "value": "图片"},
                                {"label": "视频", "value": "视频"},
                            ],
                        },
                    },
                    {
                        "label": "评论类型",
                        "value": "ctype",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "一级评论", "value": "一级评论"},
                                {"label": "二级评论", "value": "二级评论"},
                            ],
                        },
                    },
                    {
                        "label": "评论量",
                        "value": "review",
                        "type": "numbers",
                    },
                ],
            },
            {
                "text": "关注行为",
                "key": "attention",
                "fields": [
                    {
                        "label": "关注话题起始日期",
                        "value": "attention_tid_start_date",
                        "type": "datetime",
                    },
                    {
                        "label": "关注话题结束日期",
                        "value": "attention_tid_end_date",
                        "type": "datetime",
                    },
                    {
                        "label": "关注话题id",
                        "value": "attention_tid",
                        "type": "string",
                    },
                    {
                        "label": "关注用户起始日期",
                        "value": "attention_mid_start_date",
                        "type": "datetime",
                    },
                    {
                        "label": "关注用户结束日期",
                        "value": "attention_mid_end_date",
                        "type": "datetime",
                    },
                    {
                        "label": "关注用户id",
                        "value": "attention_mid",
                        "type": "string",
                    },
                ],
            },
        ],
    }

    feature_maga = {
        "web_name": "user",
        "app_name": "maga",
        "web_pack": [
            {
                "text": "用户条件",
                "key": "user",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "user_date_type",
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
                        "label": "起始日期",
                        "value": "user_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "user_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "实验id",
                        "value": "exp_id",
                        "type": "numbers",
                    },
                    {
                        "label": "实验分组",
                        "value": "exp_group",
                        "type": "numbers",
                    },
                    {
                        "label": "设备过去7天活跃天数",
                        "value": "lt7_did",
                        "type": "numbers",
                    },
                    {
                        "label": "设备过去30天活跃天数",
                        "value": "lt30_did",
                        "type": "numbers",
                    },
                    {
                        "label": "mid右龄",
                        "value": "live_days_mid",
                        "type": "numbers",
                    },
                    {
                        "label": "did右龄",
                        "value": "live_days_did",
                        "type": "numbers",
                    },
                    {
                        "label": "mid几天前登陆",
                        "value": "last_days_mid",
                        "type": "numbers",
                    },
                    {
                        "label": "did几天前登陆",
                        "value": "last_days_did",
                        "type": "numbers",
                    },
                    {
                        "label": "首次登陆方式",
                        "value": "first_launch_type",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "H5", "value": "h5"},
                                {"label": "自然启动", "value": "natural"},
                                {"label": "聊天信息", "value": "push-chat"},
                                {"label": "私信", "value": "push-msg"},
                                {"label": "帖子信息", "value": "push-post"},
                                {"label": "红点", "value": "red_icon"},
                                {"label": "其他", "value": "other"},
                            ],
                        },
                    },
                    {
                        "label": "注册日期",
                        "value": "reg_date",
                        "type": "datetime",
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
                        "label": "版本",
                        "value": "app_version",
                        "type": "string",
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
                        "label": "渠道",
                        "value": "channel_click",
                        "type": "string",
                    },
                    {
                        "label": "年龄",
                        "value": "age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "18岁及以下", "value": "18-"},
                                {"label": "19至34岁", "value": "19-34"},
                                {"label": "19至24岁", "value": "19-24"},
                                {"label": "45至60岁", "value": "45-60"},
                                {"label": "35至44岁", "value": "35-44"},
                                {"label": "60岁及以上", "value": "60+"},
                                {"label": "25至34岁", "value": "25-34"},
                                {"label": "35至60岁", "value": "35-60"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "时长(min)",
                        "value": "duration",
                        "type": "numbers",
                    },
                    {
                        "label": "启动次数",
                        "value": "session_launch",
                        "type": "numbers",
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
                        "label": "图片贴-刷帖量",
                        "value": "img_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "图片贴-消费量",
                        "value": "img_score",
                        "type": "numbers",
                    },
                    {
                        "label": "视频贴-刷帖量",
                        "value": "video_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "视频贴-消费量",
                        "value": "video_score",
                        "type": "numbers",
                    },
                    {
                        "label": "进详情次数",
                        "value": "detail_post",
                        "type": "numbers",
                    },
                    {
                        "label": "进详情时长(min)",
                        "value": "view_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "创建帖子数",
                        "value": "create_post",
                        "type": "numbers",
                    },
                    {
                        "label": "点赞帖子次数",
                        "value": "like_post",
                        "type": "numbers",
                    },
                    {
                        "label": "点踩帖子次数",
                        "value": "dislike_post",
                        "type": "numbers",
                    },
                    {
                        "label": "分享帖子次数",
                        "value": "share_post",
                        "type": "numbers",
                    },
                    {
                        "label": "收藏帖子次数",
                        "value": "favor_post",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论量",
                        "value": "create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论量",
                        "value": "reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论量",
                        "value": "total_review",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论点赞量",
                        "value": "like_create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论点赞量",
                        "value": "like_reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论总点赞量",
                        "value": "like_review",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论点踩量",
                        "value": "dislike_create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论点踩量",
                        "value": "dislike_reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论总点踩量",
                        "value": "dislike_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论分享量",
                        "value": "share_review",
                        "type": "numbers",
                    },
                    {
                        "label": "总图片浏览量",
                        "value": "total_view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "总图片浏览时长(min)",
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
                        "label": "总视频播放量",
                        "value": "total_play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "总视频播放时长(min)",
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
                ],
            },
            {
                "text": "帖子消费条件",
                "key": "post_consume",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "post_consume_date_type",
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
                        "label": "起始日期",
                        "value": "post_consume_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "post_consume_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0-5"},
                                {"label": "6~15s", "value": "6-15"},
                                {"label": "16~30s", "value": "16-30"},
                                {"label": "31~60s", "value": "31-60"},
                                {"label": "61~90s", "value": "61-90"},
                                {"label": "91~120s", "value": "91-120"},
                                {"label": "121~300s", "value": "121-300"},
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
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0天", "value": "0"},
                                {"label": "1天", "value": "1"},
                                {"label": "2天", "value": "2"},
                                {"label": "3天", "value": "3"},
                                {"label": "4~7天", "value": "4-7"},
                                {"label": "8~14天", "value": "8-14"},
                                {"label": "15~30天", "value": "15-30"},
                                {"label": "30天+", "value": "30+"},
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
                ],
            },
            {
                "text": "发帖行为",
                "key": "send_post",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "send_post_date_type",
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
                        "label": "起始日期",
                        "value": "send_post_start_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "send_post_end_date",
                        "type": "datetime",
                        "rules":[
                            {"required": True,}
                        ],
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0-5"},
                                {"label": "6~15s", "value": "6-15"},
                                {"label": "16~30s", "value": "16-30"},
                                {"label": "31~60s", "value": "31-60"},
                                {"label": "61~90s", "value": "61-90"},
                                {"label": "91~120s", "value": "91-120"},
                                {"label": "121~300s", "value": "121-300"},
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
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0天", "value": "0"},
                                {"label": "1天", "value": "1"},
                                {"label": "2天", "value": "2"},
                                {"label": "3天", "value": "3"},
                                {"label": "4~7天", "value": "4-7"},
                                {"label": "8~14天", "value": "8-14"},
                                {"label": "15~30天", "value": "15-30"},
                                {"label": "30天+", "value": "30+"},
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
                        "label": "发帖量",
                        "value": "post",
                        "type": "numbers",
                    },
                ],
            },
        ],
    }

    feature_omg = {
        "web_name": "user",
        "app_name": "omg",
        "web_pack": [
            {
                "text": "用户条件",
                "key": "user",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "user_date_type",
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
                        "label": "起始日期",
                        "value": "user_start_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "user_end_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "实验id",
                        "value": "exp_id",
                        "type": "numbers",
                    },
                    {
                        "label": "实验分组",
                        "value": "exp_group",
                        "type": "numbers",
                    },
                    {
                        "label": "设备过去7天活跃天数",
                        "value": "lt7_did",
                        "type": "numbers",
                    },
                    {
                        "label": "设备过去30天活跃天数",
                        "value": "lt30_did",
                        "type": "numbers",
                    },
                    {
                        "label": "mid右龄",
                        "value": "live_days_mid",
                        "type": "numbers",
                    },
                    {
                        "label": "did右龄",
                        "value": "live_days_did",
                        "type": "numbers",
                    },
                    {
                        "label": "mid几天前登陆",
                        "value": "last_days_mid",
                        "type": "numbers",
                    },
                    {
                        "label": "did几天前登陆",
                        "value": "last_days_did",
                        "type": "numbers",
                    },
                    {
                        "label": "注册日期",
                        "value": "reg_date",
                        "type": "datetime",
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
                        "label": "版本",
                        "value": "app_version",
                        "type": "string",
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
                        "label": "渠道",
                        "value": "channel_click",
                        "type": "string",
                    },
                    {
                        "label": "年龄",
                        "value": "age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "18岁以下", "value": "18-"},
                                {"label": "18至24岁", "value": "19-24"},
                                {"label": "25至30岁", "value": "25-30"},
                                {"label": "30岁以上", "value": "30+"},
                                {"label": "未知", "value": "unknown"},
                            ],
                        },
                    },
                    {
                        "label": "时长(min)",
                        "value": "duration",
                        "type": "numbers",
                    },
                    {
                        "label": "启动次数",
                        "value": "session_launch",
                        "type": "numbers",
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
                        "label": "图片贴-刷帖量",
                        "value": "img_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "图片贴-消费量",
                        "value": "img_score",
                        "type": "numbers",
                    },
                    {
                        "label": "视频贴-刷帖量",
                        "value": "video_expose",
                        "type": "numbers",
                    },
                    {
                        "label": "视频贴-消费量",
                        "value": "video_score",
                        "type": "numbers",
                    },
                    {
                        "label": "进详情次数",
                        "value": "detail_post",
                        "type": "numbers",
                    },
                    {
                        "label": "进详情时长(min)",
                        "value": "view_post_dur",
                        "type": "numbers",
                    },
                    {
                        "label": "创建帖子数",
                        "value": "create_post",
                        "type": "numbers",
                    },
                    {
                        "label": "点赞帖子次数",
                        "value": "like_post",
                        "type": "numbers",
                    },
                    {
                        "label": "点踩帖子次数",
                        "value": "dislike_post",
                        "type": "numbers",
                    },
                    {
                        "label": "分享帖子次数",
                        "value": "share_post",
                        "type": "numbers",
                    },
                    {
                        "label": "收藏帖子次数",
                        "value": "favor_post",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论量",
                        "value": "create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论量",
                        "value": "reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "总评论量",
                        "value": "total_review",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论点赞量",
                        "value": "like_create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论点赞量",
                        "value": "like_reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论总点赞量",
                        "value": "like_review",
                        "type": "numbers",
                    },
                    {
                        "label": "一级评论点踩量",
                        "value": "dislike_create_review",
                        "type": "numbers",
                    },
                    {
                        "label": "二级评论点踩量",
                        "value": "dislike_reply_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论总点踩量",
                        "value": "dislike_review",
                        "type": "numbers",
                    },
                    {
                        "label": "评论分享量",
                        "value": "share_review",
                        "type": "numbers",
                    },
                    {
                        "label": "总图片浏览量",
                        "value": "total_view_img",
                        "type": "numbers",
                    },
                    {
                        "label": "总图片浏览时长(min)",
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
                        "label": "总视频播放量",
                        "value": "total_play_video",
                        "type": "numbers",
                    },
                    {
                        "label": "总视频播放时长(min)",
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
                ],
            },
            {
                "text": "帖子消费条件",
                "key": "post_consume",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "post_consume_date_type",
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
                        "label": "起始日期",
                        "value": "post_consume_start_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "post_consume_end_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0-5"},
                                {"label": "6~15s", "value": "6-15"},
                                {"label": "16~30s", "value": "16-30"},
                                {"label": "31~60s", "value": "31-60"},
                                {"label": "61~90s", "value": "61-90"},
                                {"label": "91~120s", "value": "91-120"},
                                {"label": "121~300s", "value": "121-300"},
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
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0天", "value": "0"},
                                {"label": "1天", "value": "1"},
                                {"label": "2天", "value": "2"},
                                {"label": "3天", "value": "3"},
                                {"label": "4~7天", "value": "4-7"},
                                {"label": "8~14天", "value": "8-14"},
                                {"label": "15~30天", "value": "15-30"},
                                {"label": "30天+", "value": "30+"},
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
                ],
            },
            {
                "text": "发帖行为",
                "key": "send_post",
                "fields": [
                    {
                        "label": "时间类型",
                        "value": "send_post_date_type",
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
                        "label": "起始日期",
                        "value": "send_post_start_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "结束日期",
                        "value": "send_post_end_date",
                        "type": "datetime",
                        "rules": [
                            {"required": True, }
                        ],
                    },
                    {
                        "label": "帖子视频时长",
                        "value": "total_dur",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0~5s", "value": "0-5"},
                                {"label": "6~15s", "value": "6-15"},
                                {"label": "16~30s", "value": "16-30"},
                                {"label": "31~60s", "value": "31-60"},
                                {"label": "61~90s", "value": "61-90"},
                                {"label": "91~120s", "value": "91-120"},
                                {"label": "121~300s", "value": "121-300"},
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
                        "label": "贴龄",
                        "value": "post_age",
                        "type": "select",
                        "props": {
                            "mode": "multiple",
                            "options": [
                                {"label": "0天", "value": "0"},
                                {"label": "1天", "value": "1"},
                                {"label": "2天", "value": "2"},
                                {"label": "3天", "value": "3"},
                                {"label": "4~7天", "value": "4-7"},
                                {"label": "8~14天", "value": "8-14"},
                                {"label": "15~30天", "value": "15-30"},
                                {"label": "30天+", "value": "30+"},
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
                        "label": "发帖量",
                        "value": "post",
                        "type": "numbers",
                    },
                ],
            },
        ],
    }

    return feature_zy, feature_pp, feature_maga, feature_omg

def connect():
    host = "xxx"
    db = "xx"
    table = "xx"

    client = pymongo.MongoClient(host= host)
    mongodb = client[db]
    collection = mongodb[table]

    return collection

def insert(collection,values):
    collection.insert_one(values)

def delete(collection,webname):
    collection.remove({'web_name':str(webname)})

def update(collection,webname,feature):
    filter = {'web_name': str(webname)}
    newvalues = { "$set": {"web_pack" : feature}}
    collection.update_one(filter,newvalues)

if __name__ == '__main__':
    db = connect()
    value_zy, value_pp, value_maga, value_omg = data_pre()
    delete(db,'user')
    insert(db,value_zy)
    insert(db,value_pp)
    insert(db,value_maga)
    insert(db, value_omg)
