-- 类目表数据
-- 一级类目：家电、数码、服装、食品、家居
-- 二级类目：如电视、冰箱、手机、电脑等
-- 三级类目：如苹果手机、华为手机、笔记本等

-- 一级类目
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(1, '家电', 0, 1, '家电'),
(2, '数码', 0, 1, '数码'),
(3, '服装', 0, 1, '服装'),
(4, '食品', 0, 1, '食品'),
(5, '家居', 0, 1, '家居');

-- 二级类目 - 家电
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(11, '电视', 1, 2, '家电/电视'),
(12, '冰箱', 1, 2, '家电/冰箱'),
(13, '洗衣机', 1, 2, '家电/洗衣机'),
(14, '空调', 1, 2, '家电/空调');

-- 二级类目 - 数码
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(21, '手机', 2, 2, '数码/手机'),
(22, '电脑', 2, 2, '数码/电脑'),
(23, '相机', 2, 2, '数码/相机'),
(24, '配件', 2, 2, '数码/配件');

-- 二级类目 - 服装
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(31, '男装', 3, 2, '服装/男装'),
(32, '女装', 3, 2, '服装/女装'),
(33, '童装', 3, 2, '服装/童装'),
(34, '鞋靴', 3, 2, '服装/鞋靴');

-- 二级类目 - 食品
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(41, '零食', 4, 2, '食品/零食'),
(42, '饮料', 4, 2, '食品/饮料'),
(43, '生鲜', 4, 2, '食品/生鲜'),
(44, '粮油', 4, 2, '食品/粮油');

-- 二级类目 - 家居
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(51, '家具', 5, 2, '家居/家具'),
(52, '家纺', 5, 2, '家居/家纺'),
(53, '厨具', 5, 2, '家居/厨具'),
(54, '灯具', 5, 2, '家居/灯具');

-- 三级类目 - 电视
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(111, '4K电视', 11, 3, '家电/电视/4K电视'),
(112, '8K电视', 11, 3, '家电/电视/8K电视'),
(113, 'OLED电视', 11, 3, '家电/电视/OLED电视'),
(114, '智能电视', 11, 3, '家电/电视/智能电视');

-- 三级类目 - 冰箱
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(121, '双门冰箱', 12, 3, '家电/冰箱/双门冰箱'),
(122, '三门冰箱', 12, 3, '家电/冰箱/三门冰箱');

-- 三级类目 - 手机
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(211, '苹果手机', 21, 3, '数码/手机/苹果手机'),
(212, '华为手机', 21, 3, '数码/手机/华为手机'),
(213, '小米手机', 21, 3, '数码/手机/小米手机'),
(214, '三星手机', 21, 3, '数码/手机/三星手机');

-- 三级类目 - 电脑
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(221, '笔记本', 22, 3, '数码/电脑/笔记本'),
(222, '台式机', 22, 3, '数码/电脑/台式机'),
(223, '平板电脑', 22, 3, '数码/电脑/平板电脑');

-- 三级类目 - 男装
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(311, 'T恤', 31, 3, '服装/男装/T恤'),
(312, '衬衫', 31, 3, '服装/男装/衬衫'),
(313, '外套', 31, 3, '服装/男装/外套');

-- 三级类目 - 女装
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(321, '连衣裙', 32, 3, '服装/女装/连衣裙'),
(322, '上衣', 32, 3, '服装/女装/上衣'),
(323, '裤装', 32, 3, '服装/女装/裤装');

-- 三级类目 - 零食
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(411, '坚果', 41, 3, '食品/零食/坚果'),
(412, '糖果', 41, 3, '食品/零食/糖果');

-- 三级类目 - 家具
INSERT INTO categories (category_id, category_name, parent_id, level, path) VALUES
(511, '沙发', 51, 3, '家居/家具/沙发'),
(512, '床具', 51, 3, '家居/家具/床具');
