"""The TPC-BiH benchmark cases with their expected rows. Generated: do not edit.

Built by datafinder_examples/tools/build_tpch_bih_cases.py from tpch_bih_definitions.py (the report's SQL and the values of its
parameters): each case's rows are what its SQL returns on the data tpch_bih_generator.py makes (see tpch_bih_data.py).
"""
import datetime
from decimal import Decimal

from datafinder_examples_tests.tpch_bih_case import BenchmarkCase

CASES = {case.id: case for case in [
    BenchmarkCase(
        id='T1', title='Simple time-travel query on a stable relation',
        sql="""SELECT AVG(ps_supplycost), count(*)
FROM partsupp
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
FOR BUSINESS_TIME AS OF '[APP_TIME]'""",
        parameters={'SYS_TIME': datetime.datetime(2005, 6, 1, 0, 0), 'APP_TIME': datetime.date(1996, 6, 1)},
        select=(0, 1), approximate=True,
        rows=(
            (510.45436849925755, 673),
        )),

    BenchmarkCase(
        id='T2', title='Simple time-travel query on a growing relation',
        sql="""SELECT SUM(o_totalprice) AS revenue, count(*)
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE o_orderstatus = 'O'""",
        parameters={'SYS_TIME': datetime.datetime(2005, 6, 1, 0, 0), 'APP_TIME': datetime.date(1996, 6, 1)},
        select=(0, 1), approximate=True,
        rows=(
            (7200925.64, 67),
        )),

    BenchmarkCase(
        id='T5', title='Retrieving all versions',
        sql="""SELECT o_custkey, MAX(o_totalprice), count(*) AS supp_revenue
FROM orders
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30'
WHERE o_orderstatus = 'O'
GROUP BY o_custkey""",
        parameters={},
        select=('o_custkey', 1, 'supp_revenue'), approximate=True,
        rows=(
            (1, 159171.69, 8), (2, 140363.7, 5), (4, 187514.11, 13), (5, 210643.96, 9), (7, 182516.77, 1),
            (8, 163794.53, 8), (10, 258779.02, 16), (11, 109351.87, 3), (13, 159870.44, 9), (14, 182025.95, 10),
            (16, 172861.58, 7), (17, 218697.85, 5), (19, 193857.67, 8), (20, 131433.71, 9), (22, 195834.96, 10),
            (23, 98753.57, 4), (25, 204332.43, 12), (26, 143930.51, 6), (28, 244704.23, 11), (29, 241837.88, 8),
            (31, 153048.74, 16), (32, 176911.21, 8), (34, 180737.75, 9), (35, 172899.84, 9), (37, 239532.21, 13),
            (38, 166669.86, 4), (40, 189651.76, 15), (41, 173444.6, 6), (43, 216071.76, 12), (44, 145971.6, 12),
            (46, 216826.73, 9), (47, 130204.17, 7), (49, 165019.32, 15), (50, 168495.03, 9), (52, 208892.63, 14),
            (53, 176294.34, 12), (55, 219920.62, 15), (56, 204438.57, 7), (58, 138584.2, 11), (59, 82026.18, 4),
            (61, 174569.88, 11), (62, 41670.02, 2), (64, 201419.83, 17), (65, 145096.17, 10), (67, 209769.13, 13),
            (68, 120828.12, 5), (70, 263411.29, 12), (71, 47447.63, 3), (73, 178249.05, 9), (74, 173130.2, 8),
            (76, 240284.95, 13), (77, 109077.69, 6), (79, 207119.83, 12), (80, 198238.65, 6), (82, 245976.74, 6),
            (83, 159207.27, 7), (85, 209155.48, 4), (86, 111403.66, 1), (88, 176278.57, 6), (89, 132838.49, 4),
            (91, 198039.23, 13), (92, 217709.03, 3), (94, 224674.27, 17), (95, 147329.51, 5), (97, 85394.06, 4),
            (98, 220636.82, 5), (100, 187156.38, 7), (101, 204163.1, 5), (103, 198322.91, 10), (104, 131092.67, 10),
            (106, 210113.88, 15), (107, 146594.04, 3), (109, 125562.09, 8), (110, 119605.91, 9), (112, 160627.01, 9),
            (113, 193832.28, 6), (115, 157040.57, 6), (116, 108196.56, 6), (118, 202623.92, 8), (119, 186215.81, 6),
            (121, 220727.97, 10), (122, 196080.26, 8), (124, 192074.23, 8), (125, 183734.56, 11),
            (127, 139915.23, 11), (128, 190490.78, 7), (130, 148500.71, 7), (131, 237947.61, 10),
            (133, 217682.81, 9), (134, 198538.68, 8), (136, 183620.33, 13), (137, 177916.61, 5),
            (139, 216281.92, 18), (140, 183176.6, 8), (142, 167056.34, 11), (143, 156407.4, 9), (145, 223537.09, 11),
            (146, 242588.87, 5), (148, 206179.68, 14), (149, 222274.54, 20), (151, 45668.67, 2), (152, 173400.15, 2),
            (153, 172026.79, 2), (154, 119355.45, 4), (155, 98061.99, 4), (156, 167071.02, 3), (157, 35005.49, 2),
            (158, 234348.24, 3), (159, 50547.74, 2), (160, 166166.56, 2), (161, 183909.68, 3), (162, 181536.15, 2),
            (163, 149201.91, 2), (164, 32838.66, 3), (165, 112729.83, 4), (166, 114691.87, 2), (167, 140561.76, 4),
            (168, 142800.59, 6), (169, 109454.51, 2), (170, 43680.82, 3), (171, 134006.91, 3), (172, 100311.59, 2),
            (173, 48661.16, 2), (174, 105676.25, 2), (175, 196324.16, 4), (176, 83160.75, 2), (177, 84377.0, 4),
            (178, 109746.98, 5), (179, 110354.57, 3), (180, 66770.56, 2), (181, 34444.08, 2), (182, 209530.24, 4),
            (183, 77243.27, 2), (184, 97946.4, 2), (185, 117010.62, 2), (186, 114976.83, 3), (187, 109863.34, 4),
            (188, 150452.27, 2), (189, 38913.88, 2), (190, 79573.98, 2), (191, 59283.79, 2), (192, 126674.57, 2),
            (193, 166025.83, 3), (194, 178925.64, 2), (195, 36406.44, 2), (196, 80597.66, 2), (197, 112665.4, 2),
            (198, 84074.2, 2), (199, 119419.4, 3), (200, 70003.01, 2), (201, 172563.83, 2), (202, 60758.81, 2),
            (203, 131282.98, 2), (204, 149596.1, 2), (205, 29549.69, 2), (206, 11021.02, 1), (207, 175404.23, 5),
            (208, 153812.78, 3), (209, 198532.99, 3), (210, 96625.04, 2), (211, 165292.82, 6), (212, 41099.94, 2),
            (213, 60132.1, 2), (214, 166315.0, 2), (215, 27613.11, 2), (216, 221775.06, 3), (217, 6814.3, 2),
            (218, 13849.56, 2), (219, 139258.44, 2), (220, 84338.83, 3), (221, 149451.12, 2), (222, 63709.49, 2),
            (223, 112761.16, 2), (224, 189661.68, 2), (225, 52249.76, 3), (226, 84053.31, 2), (227, 152678.74, 5),
            (228, 66139.77, 2), (229, 107106.88, 3), (230, 79585.69, 4), (231, 183174.34, 4), (232, 87920.77, 2),
            (233, 140048.95, 3), (234, 100136.01, 3), (235, 110150.17, 4), (236, 192522.23, 2), (237, 98159.43, 2),
            (238, 61288.05, 2), (239, 18276.32, 1), (240, 140314.01, 2), (241, 136634.25, 1), (242, 103185.49, 2),
            (243, 24281.57, 3), (244, 111292.37, 2), (245, 128556.67, 3), (246, 32198.84, 2), (247, 81956.44, 2),
            (248, 173649.68, 2), (249, 117318.05, 3), (250, 122576.05, 2), (251, 126519.62, 1), (252, 90141.7, 1),
            (253, 20808.81, 2), (254, 177320.66, 2), (255, 181381.43, 2), (256, 33929.37, 2), (257, 26900.2, 2),
            (258, 72001.4, 1), (259, 21076.91, 2), (260, 44995.86, 2), (261, 196808.82, 2), (262, 34608.97, 2),
            (263, 30739.76, 2), (264, 144243.42, 1), (265, 98607.89, 3), (266, 101899.23, 1), (267, 42574.95, 1),
            (268, 40422.94, 2), (269, 66789.09, 3), (270, 3566.06, 2), (271, 112332.6, 1), (272, 948.19, 2),
            (273, 178285.29, 2), (274, 115074.35, 1), (275, 42888.53, 2), (276, 115768.35, 2), (277, 193083.5, 1),
            (278, 107020.23, 2), (279, 39018.43, 2), (280, 191641.43, 1), (281, 129043.02, 1), (282, 150669.32, 2),
            (283, 24318.71, 2), (284, 41963.27, 1), (285, 86800.2, 1), (286, 13508.54, 2), (287, 135206.57, 3),
            (288, 79090.45, 2), (289, 110148.83, 1), (290, 85303.7, 1), (291, 67273.17, 1), (292, 243233.9, 1),
            (293, 60921.73, 1), (294, 48754.76, 1), (295, 27997.78, 1), (296, 149927.18, 1), (297, 160395.36, 3),
            (298, 60025.27, 2), (299, 113933.0, 2), (300, 47233.23, 2), (301, 78053.32, 2), (302, 101122.37, 1),
            (303, 195221.37, 1), (304, 201099.01, 2), (305, 178381.2, 3), (306, 52508.34, 1), (307, 52005.86, 1),
            (308, 49120.15, 1), (309, 121265.62, 1), (310, 141376.88, 2), (311, 40893.32, 2), (312, 96119.48, 1),
            (313, 67198.75, 1), (314, 71272.29, 2), (315, 53111.8, 1), (316, 88878.98, 1), (317, 91424.34, 1),
            (318, 83871.52, 1), (319, 138894.02, 1), (320, 51079.27, 1), (321, 42169.38, 1), (322, 63651.51, 1),
            (323, 11929.86, 2), (324, 102776.57, 1), (325, 15802.27, 1), (326, 99054.88, 1),
        )),

    BenchmarkCase(
        id='T6_sys', title='Slicing one temporal dimension: system time',
        sql="""SELECT o_custkey, AVG(o_totalprice) AS supp_revenue
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
WHERE o_orderstatus = 'O'
GROUP BY o_custkey""",
        parameters={'SYS_TIME': datetime.datetime(2005, 6, 1, 0, 0)},
        select=('o_custkey', 'supp_revenue'), approximate=True,
        rows=(
            (1, 78602.26833333333), (2, 76479.0275), (4, 120084.28384615385), (5, 129948.86875), (7, 182516.77),
            (8, 82037.27625), (10, 86485.94999999998), (11, 55839.833333333336), (13, 94280.96111111112),
            (14, 121931.98124999998), (16, 73779.23666666665), (17, 125911.97666666668), (19, 88311.28833333333),
            (20, 102482.548), (22, 110173.32599999997), (23, 72885.41), (25, 106107.45999999999),
            (26, 81443.79800000001), (28, 95160.08222222222), (29, 154529.92125), (31, 89078.79769230771),
            (32, 99046.49124999999), (34, 70662.2711111111), (35, 92467.79875), (37, 115386.83636363636),
            (38, 117476.1475), (40, 105645.12142857142), (41, 75786.215), (43, 105366.205), (44, 75008.43727272727),
            (46, 127280.5322222222), (47, 87015.34599999999), (49, 85075.73545454544), (50, 81893.345),
            (52, 101309.43416666666), (53, 103531.45636363635), (55, 125990.25545454546), (56, 129516.71500000001),
            (58, 113606.51888888888), (59, 56980.27), (61, 93307.43181818182), (62, 32967.63),
            (64, 95604.57230769232), (65, 86477.43375), (67, 90701.24857142857), (68, 87634.06333333334),
            (70, 108682.10583333333), (71, 44120.91), (73, 71205.16444444444), (74, 94320.5225),
            (76, 155333.8823076923), (77, 65100.552500000005), (79, 120676.65916666668), (80, 141132.8),
            (82, 134715.5375), (83, 116783.51749999999), (85, 142493.06666666668), (86, 111403.66),
            (88, 103801.99333333333), (89, 63621.31), (91, 93567.12222222221), (92, 141074.07333333333),
            (94, 111542.47499999999), (95, 94222.76800000001), (97, 59452.8975), (98, 125275.25), (100, 131438.49),
            (101, 124456.59599999999), (103, 114328.15400000001), (104, 59929.810000000005), (106, 107977.89),
            (107, 118928.935), (109, 82994.20999999999), (110, 78228.656), (112, 92462.77750000001),
            (113, 85826.995), (115, 116262.76000000001), (116, 78672.53833333333), (118, 89678.685),
            (119, 101623.41), (121, 136569.8533333333), (122, 79529.6657142857), (124, 124790.1275),
            (125, 88299.18900000001), (127, 82630.95374999999), (128, 111319.495), (130, 57874.84714285714),
            (131, 116476.14833333333), (133, 131627.07571428572), (134, 130889.8175), (136, 104635.47000000002),
            (137, 107341.9025), (139, 96197.87499999999), (140, 112685.42428571428), (142, 89399.77625000002),
            (143, 87477.12428571428), (145, 88203.87818181817), (146, 164732.8525), (148, 113753.82357142858),
            (149, 133760.593125), (153, 172026.79), (154, 13151.54), (155, 98061.99), (156, 108878.83499999999),
            (157, 35005.49), (158, 234348.24), (159, 50547.74), (160, 166166.56), (161, 183909.68), (162, 181536.15),
            (165, 5600.77), (166, 114691.87), (170, 43680.82), (171, 134006.91), (172, 100311.59), (173, 48661.16),
            (174, 105676.25), (176, 83160.75), (177, 66061.64), (178, 67504.045), (179, 100847.59), (180, 66770.56),
            (182, 209530.24), (183, 77243.27), (186, 114976.83), (187, 83826.945), (189, 38913.88), (191, 59283.79),
            (192, 126674.57), (193, 166025.83), (194, 178925.64), (195, 36406.44), (196, 80597.66), (197, 112665.4),
            (198, 84074.2), (199, 119419.4), (200, 70003.01), (201, 172563.83), (202, 60758.81), (203, 131282.98),
            (204, 149596.1), (205, 29549.69), (206, 11021.02), (207, 97882.82), (208, 97907.66), (209, 123438.2),
            (210, 96625.04), (211, 87724.47), (212, 41099.94), (214, 166315.0), (215, 27613.11), (216, 96992.08),
            (217, 6814.3), (218, 13849.56), (219, 139258.44), (220, 66119.16500000001), (221, 149451.12),
            (222, 63709.49), (223, 112761.16), (224, 189661.68), (225, 43916.57), (226, 84053.31), (227, 26998.58),
            (228, 66139.77), (230, 79268.19), (231, 183174.34), (232, 87920.77), (233, 140048.95), (234, 31563.86),
            (235, 1032.7), (236, 192522.23), (237, 98159.43), (238, 61288.05), (239, 18276.32), (240, 140314.01),
            (241, 136634.25), (242, 103185.49), (243, 24281.57), (244, 111292.37), (245, 68832.58),
        )),

    BenchmarkCase(
        id='T6_app', title='Slicing one temporal dimension: application time',
        sql="""SELECT o_custkey, AVG(o_totalprice) AS supp_revenue
FROM orders
FOR BUSINESS_TIME AS OF TIMESTAMP '[APP_TIME]'
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30'
WHERE o_orderstatus = 'O'
GROUP BY o_custkey""",
        parameters={'APP_TIME': datetime.date(1996, 6, 1)},
        select=('o_custkey', 'supp_revenue'), approximate=True,
        rows=(
            (4, 3892.77), (5, 77754.62), (10, 65568.274), (13, 80066.575), (14, 147343.68), (17, 218697.85),
            (20, 86575.095), (22, 128786.57), (26, 62567.99), (28, 104251.7), (34, 50724.06), (40, 128560.83),
            (41, 65385.42), (43, 141159.63), (44, 143212.85), (46, 180054.29), (49, 54797.575000000004),
            (52, 185968.15), (53, 66512.99), (61, 151801.06), (64, 154832.28), (65, 64000.93), (70, 79863.84),
            (74, 113156.3), (76, 104585.77), (79, 132151.04), (85, 119820.38), (91, 173145.37),
            (94, 127162.78666666667), (98, 68619.29), (104, 37402.175), (109, 82824.14), (112, 84871.5),
            (113, 85755.84), (115, 119595.845), (116, 80109.45), (121, 161767.78), (124, 158776.68), (125, 76860.53),
            (127, 136634.34), (133, 100795.20999999999), (139, 157062.7), (148, 128755.20666666667),
            (149, 142122.03666666665),
        )),

    BenchmarkCase(
        id='T7', title='Implicit vs. explicit current time',
        sql="""SELECT PS_PARTKEY, AVG(ps_availqty) AS avail
FROM partsupp
GROUP BY PS_PARTKEY""",
        parameters={},
        select=('ps_partkey', 'avail'), approximate=True,
        rows=(
            (1, 4850.5), (2, 6338.25), (3, 5641.5), (4, 3216.0), (5, 5390.25), (6, 5041.5), (7, 5765.25),
            (8, 6292.5), (9, 6807.75), (10, 3185.0), (11, 4015.5), (12, 4364.5), (13, 4620.0), (14, 4791.75),
            (15, 4684.0), (16, 4247.5), (17, 5642.75), (18, 4684.5), (19, 4255.5), (20, 3956.5), (21, 3925.25),
            (22, 5838.0), (23, 6330.0), (24, 4960.0), (25, 8834.0), (26, 6172.5), (27, 4713.75), (28, 4830.5),
            (29, 6742.5), (30, 5246.5), (31, 5779.5), (32, 1326.5), (33, 4182.0), (34, 7327.5), (35, 5676.0),
            (36, 2009.0), (37, 6327.0), (38, 2711.5), (39, 3645.5), (40, 4641.0), (41, 4273.5), (42, 3238.75),
            (43, 5653.0), (44, 3532.0), (45, 3603.75), (46, 5316.75), (47, 5029.25), (48, 5442.25), (49, 5404.75),
            (50, 2540.75), (51, 4049.75), (52, 4503.75), (53, 6722.75), (54, 3099.75), (55, 6434.25), (56, 4091.0),
            (57, 3990.75), (58, 5600.25), (59, 5206.25), (60, 4392.5), (61, 5684.5), (62, 6586.25), (63, 4209.0),
            (64, 5302.0), (65, 2214.25), (66, 1748.0), (67, 6745.25), (68, 5964.5), (69, 7685.75), (70, 6148.5),
            (71, 5196.0), (72, 6574.5), (73, 6707.5), (74, 5323.0), (75, 6975.75), (76, 5753.25), (77, 4735.5),
            (78, 7129.5), (79, 4160.0), (80, 5133.75), (81, 1529.0), (82, 7787.0), (83, 2937.0), (84, 5657.0),
            (85, 2534.0), (86, 763.0), (87, 5552.0), (88, 6084.0), (89, 3389.0), (90, 8037.0), (91, 3235.0),
            (92, 3925.5), (93, 4834.0), (94, 5922.25), (95, 4854.75), (96, 6957.0), (97, 3970.0), (98, 6301.5),
            (99, 6680.25), (100, 6139.75), (101, 5038.5), (102, 6641.0), (103, 4778.75), (104, 4509.0),
            (105, 5552.75), (106, 3839.25), (107, 5952.75), (108, 4431.5), (109, 5166.25), (110, 4429.5),
            (111, 5400.75), (112, 5550.0), (113, 4741.0), (114, 4207.75), (115, 3385.0), (116, 6464.75),
            (117, 3794.75), (118, 4936.5), (119, 2310.5), (120, 5282.75), (121, 7576.5), (122, 2631.5),
            (123, 5871.5), (124, 7437.75), (125, 4660.25), (126, 2968.0), (127, 6562.0), (128, 6331.5),
            (129, 5047.75), (130, 6002.5), (131, 1684.5), (132, 2495.0), (133, 3011.5), (134, 8860.0), (135, 4669.5),
            (136, 4140.0), (137, 6567.5), (138, 1269.5), (139, 6062.5), (140, 2938.5), (141, 4016.5), (142, 4035.25),
            (143, 5076.75), (144, 3488.5), (145, 2388.75), (146, 5714.5), (147, 5222.25), (148, 4552.75),
            (149, 4924.5), (150, 3861.25), (151, 2870.75), (152, 2210.5), (153, 5615.0), (154, 4037.25),
            (155, 3488.25), (156, 4550.0), (157, 3316.75), (158, 5055.5), (159, 5629.75), (160, 5898.0),
            (161, 7014.0), (162, 3568.25), (163, 6902.75), (164, 2418.75), (165, 4953.25), (166, 7245.0),
            (167, 5557.0), (168, 4538.25), (169, 6951.75), (170, 5360.25), (171, 6665.0), (172, 5844.5),
            (173, 5743.25), (174, 4351.0), (175, 7745.5), (176, 4474.5), (177, 5024.0), (178, 4950.25),
            (179, 4099.0), (180, 2522.5), (181, 2383.0), (182, 9667.0), (183, 100.0), (184, 7000.0), (185, 1454.0),
            (186, 1054.0), (187, 8590.0), (188, 4798.0), (189, 1263.0), (190, 487.0), (191, 5030.0), (192, 3326.0),
            (193, 5256.75), (194, 4456.0), (195, 4004.25), (196, 3036.0), (197, 7206.75), (198, 6975.5),
            (199, 6831.5), (200, 4551.5),
        )),

    BenchmarkCase(
        id='T8', title='Time travel among multiple application times',
        sql="""SELECT count(*)
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
FOR BUSINESS_TIME AS OF '[APP_TIME]'
WHERE '[APP_TIME]' BETWEEN receivable_time_start AND receivable_time_end""",
        parameters={'SYS_TIME': datetime.datetime(2005, 6, 1, 0, 0), 'APP_TIME': datetime.date(1996, 6, 1)},
        select=(0,),
        rows=(
            (10,),
        )),

    BenchmarkCase(
        id='T9', title='Time travel to a simulated/secondary application time',
        sql="""SELECT count(*)
FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
WHERE '[APP_TIME]' BETWEEN receivable_time_start AND receivable_time_end""",
        parameters={'SYS_TIME': datetime.datetime(2005, 6, 1, 0, 0), 'APP_TIME': datetime.date(1996, 6, 1)},
        select=(0,),
        rows=(
            (10,),
        )),

    BenchmarkCase(
        id='K1_sys', title='All time versions of a tuple: system time',
        sql="""SELECT
o_orderkey, o_custkey, o_orderstatus, o_totalprice,
o_orderdate, o_orderpriority, o_shippriority,
sys_time_start
FROM
orders
FOR
SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
o_orderkey = [ORDER_KEY]
ORDER BY
sys_time_start""",
        parameters={'ORDER_KEY': 6052, 'APP_TIME': datetime.date(2002, 1, 1)},
        select=('o_orderkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_shippriority', 'sys_time_begin'), ordered=True, approximate=True,
        rows=(
            (6052, 'O', 119355.45, datetime.date(2001, 11, 28), '2-HIGH', 0, datetime.datetime(2001, 11, 28, 12, 46, 4)),
            (6052, 'O', 119355.45, datetime.date(2001, 11, 28), '2-HIGH', 0, datetime.datetime(2004, 1, 4, 10, 50, 52)),
            (6052, 'P', 119355.45, datetime.date(2001, 11, 28), '2-HIGH', 0, datetime.datetime(2004, 1, 8, 2, 29, 45)),
        )),

    BenchmarkCase(
        id='K1_app', title='All time versions of a tuple: application time',
        sql="""SELECT
o_orderkey, o_custkey, o_orderstatus, o_totalprice,
o_orderdate, o_orderpriority, o_shippriority,
active_time_start
FROM
orders
FOR
BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE
o_orderkey = [ORDER_KEY]
ORDER BY
active_time_start""",
        parameters={'ORDER_KEY': 6052},
        select=('o_orderkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_shippriority', 'active_time_begin'), ordered=True, approximate=True,
        rows=(
            (6052, 'P', 119355.45, datetime.date(2001, 11, 28), '2-HIGH', 0, datetime.date(2001, 11, 28)),
        )),

    BenchmarkCase(
        id='K2_sys', title='A time version range of a tuple: system time',
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
sys_time_start
FROM
customer
FOR
SYSTEM_TIME FROM TIMESTAMP '[SYS_TIME_BEGIN]' TO TIMESTAMP '[SYS_TIME_END]'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_custkey = [CUSTOMER_KEY]
ORDER BY
sys_time_start""",
        parameters={'CUSTOMER_KEY': 67, 'APP_TIME': datetime.date(2004, 1, 1), 'SYS_TIME_BEGIN': datetime.datetime(2003, 1, 1, 0, 0), 'SYS_TIME_END': datetime.datetime(2009, 1, 1, 0, 0)},
        select=('c_custkey', 'c_name', 'c_address', 'c_phone', 'c_acctbal', 'sys_time_begin'), ordered=True, approximate=True,
        rows=(
            (67, 'Customer#000000067', 'ALk,OdKy V5SruigGvBRMBo8S', '19-403-114-4356', 8166.59, datetime.datetime(2000, 11, 17, 9, 1, 26)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 8166.59, datetime.datetime(2003, 9, 9, 14, 6, 43)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 217935.72, datetime.datetime(2004, 6, 5, 20, 3, 50)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 34649.39, datetime.datetime(2004, 9, 16, 2, 12, 28)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 195574.87, datetime.datetime(2004, 10, 26, 6, 20, 9)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 125171.25, datetime.datetime(2005, 7, 5, 6, 2, 52)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 45790.74, datetime.datetime(2005, 7, 8, 21, 41, 45)),
        )),

    BenchmarkCase(
        id='K2_app', title='A time version range of a tuple: application time',
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
visible_time_start
FROM
customer
FOR
BUSINESS_TIME FROM '[APP_TIME_BEGIN]' TO '[APP_TIME_END]'
WHERE
c_custkey = [CUSTOMER_KEY]
ORDER BY
visible_time_start""",
        parameters={'CUSTOMER_KEY': 67, 'APP_TIME_BEGIN': datetime.date(1992, 1, 1), 'APP_TIME_END': datetime.date(2010, 1, 1)},
        select=('c_custkey', 'c_name', 'c_address', 'c_phone', 'c_acctbal', 'visible_time_begin'), ordered=True, approximate=True,
        rows=(
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 45790.74, datetime.date(2003, 9, 28)),
        )),

    BenchmarkCase(
        id='K3_sys', title='Aggregation over system time',
        sql="""SELECT
MIN(c_acctbal)
FROM
customer
FOR
SYSTEM_TIME FROM TIMESTAMP '[SYS_TIME_BEGIN]' TO TIMESTAMP '[SYS_TIME_END]'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_custkey = [CUSTOMER_KEY]""",
        parameters={'CUSTOMER_KEY': 67, 'APP_TIME': datetime.date(2004, 1, 1), 'SYS_TIME_BEGIN': datetime.datetime(2003, 1, 1, 0, 0), 'SYS_TIME_END': datetime.datetime(2009, 1, 1, 0, 0)},
        select=(0,), approximate=True,
        rows=(
            (8166.59,),
        )),

    BenchmarkCase(
        id='K3_app', title='Aggregation over application time',
        sql="""SELECT
MIN(c_acctbal)
FROM
customer
FOR
BUSINESS_TIME FROM '[APP_TIME_BEGIN]' TO '[APP_TIME_END]'
WHERE
c_custkey = [CUSTOMER_KEY]""",
        parameters={'CUSTOMER_KEY': 67, 'APP_TIME_BEGIN': datetime.date(1992, 1, 1), 'APP_TIME_END': datetime.date(2010, 1, 1)},
        select=(0,), approximate=True,
        rows=(
            (45790.74,),
        )),

    BenchmarkCase(
        id='K4_sys', title='A version count range of a tuple: system time',
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
sys_time_start, sys_time_end
FROM
customer
FOR
SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_custkey = [CUSTOMER_KEY]
ORDER BY
sys_time_start
FETCH FIRST [HSIZE] ROWS ONLY""",
        parameters={'CUSTOMER_KEY': 67, 'APP_TIME': datetime.date(2004, 1, 1), 'HSIZE': 10},
        select=('c_custkey', 'c_name', 'c_address', 'c_phone', 'c_acctbal', 'sys_time_begin', 'sys_time_end'), ordered=True, approximate=True,
        rows=(
            (67, 'Customer#000000067', 'ALk,OdKy V5SruigGvBRMBo8S', '19-403-114-4356', 8166.59, datetime.datetime(2000, 1, 1, 0, 0), datetime.datetime(2000, 3, 21, 8, 15, 21)),
            (67, 'Customer#000000067', 'ALk,OdKy V5SruigGvBRMBo8S', '19-403-114-4356', 80639.05, datetime.datetime(2000, 3, 21, 8, 15, 21), datetime.datetime(2000, 11, 17, 9, 1, 26)),
            (67, 'Customer#000000067', 'ALk,OdKy V5SruigGvBRMBo8S', '19-403-114-4356', 8166.59, datetime.datetime(2000, 11, 17, 9, 1, 26), datetime.datetime(2003, 9, 9, 14, 6, 43)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 8166.59, datetime.datetime(2003, 9, 9, 14, 6, 43), datetime.datetime(2004, 6, 5, 20, 3, 50)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 217935.72, datetime.datetime(2004, 6, 5, 20, 3, 50), datetime.datetime(2004, 9, 16, 2, 12, 28)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 34649.39, datetime.datetime(2004, 9, 16, 2, 12, 28), datetime.datetime(2004, 10, 26, 6, 20, 9)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 195574.87, datetime.datetime(2004, 10, 26, 6, 20, 9), datetime.datetime(2005, 7, 5, 6, 2, 52)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 125171.25, datetime.datetime(2005, 7, 5, 6, 2, 52), datetime.datetime(2005, 7, 8, 21, 41, 45)),
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 45790.74, datetime.datetime(2005, 7, 8, 21, 41, 45), datetime.datetime(9999, 12, 31, 23, 59, 59)),
        )),

    BenchmarkCase(
        id='K4_app', title='A version count range of a tuple: application time',
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
visible_time_start, visible_time_end
FROM
customer
FOR
BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE
c_custkey = [CUST_KEY]
ORDER BY
visible_time_start
FETCH FIRST [HSIZE] ROWS ONLY""",
        parameters={'CUST_KEY': 67, 'HSIZE': 10},
        select=('c_custkey', 'c_name', 'c_address', 'c_phone', 'c_acctbal', 'visible_time_begin', 'visible_time_end'), ordered=True, approximate=True,
        rows=(
            (67, 'Customer#000000067', 'final final packages', '14-513-189-4539', 45790.74, datetime.date(2003, 9, 28), datetime.date(9999, 12, 31)),
        )),

    BenchmarkCase(
        id='K5_sys', title='The preceding version of a tuple: system time',
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
sys_time_start
FROM
customer
FOR
SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_custkey = [CUSTOMER_KEY] AND
sys_time_end =
(
SELECT max(sys_time_end)
FROM customer
FOR
SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_custkey = [CUSTOMER_KEY] and sys_time_END <= TIMESTAMP '[SYS_TIME]'
)""",
        parameters={'CUSTOMER_KEY': 67, 'APP_TIME': datetime.date(2004, 1, 1), 'SYS_TIME': datetime.datetime(2006, 1, 1, 0, 0)},
        select=('c_custkey', 'c_acctbal', 'sys_time_begin'), approximate=True,
        rows=(
            (67, 125171.25, datetime.datetime(2005, 7, 5, 6, 2, 52)),
        )),

    BenchmarkCase(
        id='K5_app', title='The preceding version of a tuple: application time',
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
visible_time_start
FROM
customer
WHERE
c_custkey = [CUSTOMER_KEY] AND
visible_time_end =
(
SELECT max(visible_time_end)
FROM customer
WHERE
c_custkey = [CUSTOMER_KEY] and VISIBLE_TIME_END <= '[APP_TIME]'
)""",
        parameters={'CUSTOMER_KEY': 67, 'APP_TIME': datetime.date(2010, 1, 1)},
        select=('c_custkey', 'c_acctbal', 'visible_time_begin'), approximate=True,
        rows=(

        )),

    BenchmarkCase(
        id='K6_sys', title='All versions of the tuples selected by value: system time',
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
sys_time_start
FROM
customer
FOR
SYSTEM_TIME FROM '0001-01-01' TO '9999-12-30'
FOR
BUSINESS_TIME AS OF '[APP_TIME]'
WHERE
c_acctbal > [CUST_BAL]
ORDER BY
c_custkey, c_acctbal, sys_time_start""",
        parameters={'APP_TIME': datetime.date(2004, 1, 1), 'CUST_BAL': 9990},
        select=('c_custkey', 'c_acctbal', 'sys_time_begin'), ordered=True, approximate=True,
        rows=(
            (1, 37854.77, datetime.datetime(2003, 9, 20, 13, 3, 21)),
            (1, 136918.05, datetime.datetime(2008, 7, 1, 4, 48)),
            (2, 77438.53, datetime.datetime(2003, 4, 20, 3, 50, 24)),
            (5, 40538.6, datetime.datetime(2004, 3, 13, 20, 9, 36)),
            (14, 47118.46, datetime.datetime(2004, 8, 14, 5, 22, 33)),
            (14, 126621.03, datetime.datetime(2003, 12, 20, 20, 15, 21)),
            (17, 124628.68, datetime.datetime(2006, 3, 6, 22, 27, 50)),
            (19, 163091.56, datetime.datetime(2003, 11, 25, 6, 43, 12)),
            (20, 107097.94, datetime.datetime(2002, 7, 23, 21, 53, 16)),
            (23, 74756.14, datetime.datetime(2009, 3, 2, 21, 12, 57)),
            (28, 20799.28, datetime.datetime(2005, 7, 1, 14, 24)),
            (28, 33997.06, datetime.datetime(2001, 10, 30, 7, 35, 2)),
            (28, 44160.62, datetime.datetime(2001, 2, 2, 1, 37, 55)),
            (35, 13217.99, datetime.datetime(2007, 10, 26, 20, 44, 9)),
            (35, 20449.9, datetime.datetime(2002, 3, 18, 2, 12, 28)),
            (40, 52421.8, datetime.datetime(2004, 9, 27, 1, 9, 7)),
            (47, 75988.11, datetime.datetime(2001, 4, 23, 9, 53, 16)),
            (49, 10207.52, datetime.datetime(2007, 10, 12, 6, 8, 38)),
            (49, 16609.93, datetime.datetime(2005, 5, 4, 4, 1, 55)),
            (49, 25554.96, datetime.datetime(2003, 11, 28, 22, 22, 4)),
            (49, 36006.93, datetime.datetime(2002, 4, 23, 14, 41, 16)),
            (49, 93320.2, datetime.datetime(2008, 7, 26, 18, 20, 9)),
            (55, 86820.39, datetime.datetime(2001, 11, 6, 14, 52, 48)),
            (55, 86820.39, datetime.datetime(2003, 5, 15, 17, 22, 33)),
            (55, 169313.46, datetime.datetime(2001, 7, 31, 0, 23, 2)),
            (55, 254647.65, datetime.datetime(2004, 12, 9, 2, 6, 43)),
            (58, 83844.49, datetime.datetime(2003, 7, 5, 20, 26, 52)),
            (64, 23289.57, datetime.datetime(2004, 5, 11, 6, 31, 40)),
            (64, 67178.74, datetime.datetime(2003, 11, 14, 7, 46, 33)),
            (64, 90654.86, datetime.datetime(2002, 11, 10, 11, 19, 40)),
            (64, 189611.68, datetime.datetime(2002, 11, 6, 19, 40, 48)),
            (67, 34649.39, datetime.datetime(2004, 9, 16, 2, 12, 28)),
            (67, 45790.74, datetime.datetime(2005, 7, 8, 21, 41, 45)),
            (67, 80639.05, datetime.datetime(2000, 3, 21, 8, 15, 21)),
            (67, 125171.25, datetime.datetime(2005, 7, 5, 6, 2, 52)),
            (67, 195574.87, datetime.datetime(2004, 10, 26, 6, 20, 9)),
            (67, 217935.72, datetime.datetime(2004, 6, 5, 20, 3, 50)),
            (68, 70055.47, datetime.datetime(2002, 3, 10, 18, 54, 43)),
            (70, 46625.96, datetime.datetime(2009, 7, 23, 7, 29, 16)),
            (71, 40183.0, datetime.datetime(2001, 5, 8, 0, 28, 48)),
            (77, 10877.56, datetime.datetime(2006, 9, 16, 11, 48, 28)),
            (77, 115005.58, datetime.datetime(2007, 9, 16, 16, 36, 28)),
            (83, 43798.7, datetime.datetime(2003, 12, 6, 5, 39, 50)),
            (83, 79593.92, datetime.datetime(2002, 8, 11, 4, 7, 40)),
            (83, 165670.78, datetime.datetime(2002, 5, 15, 12, 34, 33)),
            (91, 13878.8, datetime.datetime(2007, 12, 24, 7, 6, 14)),
            (91, 132963.25, datetime.datetime(2004, 11, 9, 20, 55, 40)),
            (106, 28436.79, datetime.datetime(2004, 7, 30, 14, 47, 2)),
            (110, 66669.62, datetime.datetime(2004, 8, 6, 22, 4, 48)),
            (112, 79025.93, datetime.datetime(2001, 8, 21, 22, 16, 19)),
            (119, 70447.96, datetime.datetime(2001, 5, 18, 23, 25, 26)),
            (122, 16366.58, datetime.datetime(2003, 8, 4, 1, 37, 55)),
            (127, 25705.38, datetime.datetime(2003, 9, 27, 20, 21, 7)),
            (127, 69464.49, datetime.datetime(2005, 12, 23, 21, 30, 14)),
            (127, 86387.0, datetime.datetime(2004, 1, 19, 1, 26, 24)),
            (127, 115747.79, datetime.datetime(2002, 4, 30, 21, 59, 2)),
            (128, 61008.57, datetime.datetime(2003, 2, 28, 0, 46, 4)),
            (131, 54589.06, datetime.datetime(2001, 2, 16, 16, 13, 26)),
            (131, 165436.49, datetime.datetime(2002, 9, 2, 2, 0, 57)),
            (133, 88455.48, datetime.datetime(2001, 9, 20, 3, 27, 21)),
            (134, 114072.79, datetime.datetime(2001, 9, 5, 12, 51, 50)),
            (134, 172066.33, datetime.datetime(2007, 1, 18, 15, 50, 24)),
            (136, 92018.59, datetime.datetime(2001, 7, 9, 2, 29, 45)),
            (136, 92018.59, datetime.datetime(2002, 6, 21, 1, 3, 21)),
            (136, 140738.02, datetime.datetime(2009, 3, 6, 12, 51, 50)),
            (136, 149696.67, datetime.datetime(2003, 8, 29, 15, 10, 4)),
            (136, 242557.65, datetime.datetime(2003, 6, 2, 23, 36, 57)),
            (139, 88853.15, datetime.datetime(2005, 3, 14, 0, 57, 36)),
            (139, 185311.18, datetime.datetime(2004, 1, 22, 17, 5, 16)),
            (139, 218707.53, datetime.datetime(2003, 12, 31, 19, 12)),
            (142, 11582.26, datetime.datetime(2003, 12, 13, 12, 57, 36)),
            (142, 33876.77, datetime.datetime(2003, 8, 25, 23, 31, 12)),
            (142, 200933.11, datetime.datetime(2002, 7, 27, 13, 32, 9)),
            (143, 111288.98, datetime.datetime(2001, 8, 7, 7, 40, 48)),
            (149, 18050.23, datetime.datetime(2003, 11, 21, 15, 4, 19)),
            (149, 38797.9, datetime.datetime(2004, 11, 20, 19, 52, 19)),
            (149, 80857.36, datetime.datetime(2002, 12, 13, 8, 9, 36)),
            (149, 80857.36, datetime.datetime(2003, 10, 27, 1, 32, 9)),
            (149, 168454.98, datetime.datetime(2004, 9, 8, 18, 54, 43)),
            (151, 45668.67, datetime.datetime(2000, 2, 24, 18, 43, 12)),
            (152, 173400.15, datetime.datetime(2000, 7, 30, 19, 35, 2)),
            (153, 172026.79, datetime.datetime(2000, 4, 15, 21, 47, 31)),
            (154, 13151.54, datetime.datetime(2001, 2, 13, 0, 34, 33)),
            (154, 13151.54, datetime.datetime(2001, 11, 28, 12, 46, 4)),
            (154, 13151.54, datetime.datetime(2004, 1, 8, 2, 29, 45)),
            (154, 132506.99, datetime.datetime(2004, 1, 4, 10, 50, 52)),
            (155, 70022.48, datetime.datetime(2001, 9, 1, 21, 12, 57)),
            (155, 98061.99, datetime.datetime(2003, 3, 10, 23, 42, 43)),
            (156, 167071.02, datetime.datetime(2007, 2, 16, 21, 1, 26)),
            (157, 35005.49, datetime.datetime(2001, 4, 27, 1, 32, 9)),
            (158, 234348.24, datetime.datetime(2000, 6, 9, 16, 30, 43)),
            (159, 50547.74, datetime.datetime(2004, 11, 17, 4, 13, 26)),
            (160, 166166.56, datetime.datetime(2001, 1, 7, 12, 5, 45)),
            (161, 104060.07, datetime.datetime(2001, 6, 24, 11, 54, 14)),
            (161, 183909.68, datetime.datetime(2000, 7, 5, 6, 2, 52)),
            (162, 181536.15, datetime.datetime(2003, 7, 9, 12, 5, 45)),
            (163, 149201.91, datetime.datetime(2002, 6, 10, 2, 6, 43)),
            (164, 12849.09, datetime.datetime(2001, 5, 26, 6, 43, 12)),
            (164, 12849.09, datetime.datetime(2008, 1, 7, 21, 41, 45)),
            (165, 118330.6, datetime.datetime(2003, 7, 27, 18, 20, 9)),
            (166, 114691.87, datetime.datetime(2001, 5, 15, 7, 46, 33)),
            (167, 109600.37, datetime.datetime(2000, 10, 4, 13, 14, 52)),
            (168, 142800.59, datetime.datetime(2003, 6, 21, 5, 51, 21)),
            (169, 109454.51, datetime.datetime(2002, 4, 9, 0, 5, 45)),
            (170, 43680.82, datetime.datetime(2000, 11, 28, 7, 58, 4)),
            (170, 43680.82, datetime.datetime(2002, 5, 4, 13, 37, 55)),
            (171, 60726.38, datetime.datetime(2008, 4, 15, 12, 11, 31)),
            (171, 134006.91, datetime.datetime(2002, 10, 19, 13, 26, 24)),
            (172, 100311.59, datetime.datetime(2003, 4, 5, 13, 14, 52)),
            (173, 48661.16, datetime.datetime(2001, 11, 21, 5, 28, 19)),
            (174, 105676.25, datetime.datetime(2001, 6, 20, 20, 15, 21)),
            (175, 140907.32, datetime.datetime(2009, 5, 22, 5, 28, 19)),
            (175, 196324.16, datetime.datetime(2001, 2, 5, 17, 16, 48)),
            (176, 83160.75, datetime.datetime(2007, 4, 23, 14, 41, 16)),
            (177, 66061.64, datetime.datetime(2001, 6, 17, 4, 36, 28)),
            (178, 101020.51, datetime.datetime(2001, 5, 4, 8, 49, 55)),
            (178, 101020.51, datetime.datetime(2003, 5, 4, 18, 25, 55)),
            (179, 100847.59, datetime.datetime(2002, 7, 20, 6, 14, 24)),
            (180, 66770.56, datetime.datetime(2004, 6, 13, 3, 21, 36)),
            (181, 34444.08, datetime.datetime(2002, 3, 21, 17, 51, 21)),
            (182, 32608.4, datetime.datetime(2007, 8, 22, 3, 4, 19)),
            (182, 209530.24, datetime.datetime(2002, 10, 4, 22, 50, 52)),
            (183, 77243.27, datetime.datetime(2002, 12, 24, 7, 6, 14)),
            (184, 97946.4, datetime.datetime(2004, 3, 28, 10, 45, 7)),
            (185, 117010.62, datetime.datetime(2002, 6, 13, 17, 45, 36)),
            (186, 114976.83, datetime.datetime(2002, 6, 17, 9, 24, 28)),
            (187, 57790.55, datetime.datetime(2003, 1, 22, 12, 17, 16)),
            (188, 150452.27, datetime.datetime(2003, 8, 15, 0, 34, 33)),
            (189, 38913.88, datetime.datetime(2002, 12, 31, 14, 24)),
            (190, 79573.98, datetime.datetime(2003, 2, 6, 2, 52, 48)),
            (191, 59283.79, datetime.datetime(2002, 12, 9, 16, 30, 43)),
            (192, 126674.57, datetime.datetime(2004, 3, 21, 3, 27, 21)),
            (193, 166025.83, datetime.datetime(2004, 2, 24, 13, 55, 12)),
            (194, 178925.64, datetime.datetime(2004, 2, 17, 6, 37, 26)),
            (195, 36406.44, datetime.datetime(2006, 10, 4, 18, 2, 52)),
            (196, 80597.66, datetime.datetime(2003, 1, 29, 19, 35, 2)),
            (197, 112665.4, datetime.datetime(2008, 9, 23, 4, 42, 14)),
            (198, 84074.2, datetime.datetime(2004, 10, 22, 14, 41, 16)),
            (199, 119419.4, datetime.datetime(2005, 6, 16, 23, 48, 28)),
            (200, 70003.01, datetime.datetime(2003, 2, 17, 1, 49, 26)),
            (201, 172563.83, datetime.datetime(2003, 3, 7, 8, 3, 50)),
            (202, 60758.81, datetime.datetime(2009, 7, 26, 23, 8, 9)),
            (203, 131282.98, datetime.datetime(2005, 5, 7, 19, 40, 48)),
            (204, 149596.1, datetime.datetime(2007, 1, 25, 23, 8, 9)),
            (205, 29549.69, datetime.datetime(2008, 4, 11, 20, 32, 38)),
            (207, 97882.82, datetime.datetime(2004, 7, 26, 23, 8, 9)),
            (207, 111875.22, datetime.datetime(2007, 12, 16, 23, 48, 28)),
            (207, 209758.04, datetime.datetime(2007, 9, 27, 15, 33, 7)),
            (208, 97907.66, datetime.datetime(2003, 10, 8, 19, 17, 45)),
            (209, 123438.2, datetime.datetime(2003, 6, 17, 14, 12, 28)),
            (210, 96625.04, datetime.datetime(2007, 6, 2, 18, 48, 57)),
            (212, 41099.94, datetime.datetime(2003, 12, 24, 11, 54, 14)),
            (213, 60132.1, datetime.datetime(2003, 5, 12, 1, 43, 40)),
            (213, 60132.1, datetime.datetime(2009, 8, 28, 19, 58, 4)),
            (214, 166315.0, datetime.datetime(2004, 5, 7, 14, 52, 48)),
            (215, 27613.11, datetime.datetime(2003, 8, 22, 7, 52, 19)),
            (216, 96992.08, datetime.datetime(2005, 1, 14, 14, 35, 31)),
            (218, 13849.56, datetime.datetime(2004, 6, 2, 4, 24, 57)),
            (219, 139258.44, datetime.datetime(2006, 2, 23, 23, 31, 12)),
            (220, 36439.33, datetime.datetime(2006, 8, 10, 23, 19, 40)),
            (220, 84338.83, datetime.datetime(2004, 11, 13, 12, 34, 33)),
            (221, 149451.12, datetime.datetime(2008, 10, 4, 3, 38, 52)),
            (222, 63709.49, datetime.datetime(2004, 3, 10, 4, 30, 43)),
            (223, 112761.16, datetime.datetime(2008, 4, 8, 4, 53, 45)),
        )),

    BenchmarkCase(
        id='K6_app', title='All versions of the tuples selected by value: application time',
        sql="""SELECT
c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
visible_time_start
FROM
customer
FOR
BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30'
WHERE
c_acctbal > [CUST_BAL]
ORDER BY
c_custkey, c_acctbal, visible_time_start""",
        parameters={'CUST_BAL': 9990},
        select=('c_custkey', 'c_acctbal', 'visible_time_begin'), ordered=True, approximate=True,
        rows=(
            (1, 136918.05, datetime.date(1992, 10, 21)), (2, 77438.53, datetime.date(1992, 7, 8)),
            (5, 40538.6, datetime.date(2003, 10, 29)), (10, 160676.52, datetime.date(2007, 4, 7)),
            (14, 47118.46, datetime.date(1992, 4, 19)), (20, 128137.74, datetime.date(2006, 8, 6)),
            (23, 74756.14, datetime.date(1992, 1, 13)), (28, 20799.28, datetime.date(2000, 12, 4)),
            (31, 53459.56, datetime.date(2006, 7, 15)), (35, 13217.99, datetime.date(1992, 1, 13)),
            (37, 80135.09, datetime.date(2005, 5, 2)), (40, 52421.8, datetime.date(2003, 9, 29)),
            (44, 68448.75, datetime.date(2004, 5, 15)), (49, 93320.2, datetime.date(2000, 11, 4)),
            (52, 11360.08, datetime.date(2006, 5, 5)), (53, 23010.88, datetime.date(2004, 2, 29)),
            (55, 40439.7, datetime.date(2006, 2, 28)), (58, 83844.49, datetime.date(2009, 12, 3)),
            (64, 23289.57, datetime.date(2002, 3, 14)), (67, 45790.74, datetime.date(2003, 9, 28)),
            (70, 46625.96, datetime.date(1992, 1, 4)), (71, 40183.0, datetime.date(2001, 2, 5)),
            (77, 115005.58, datetime.date(2002, 8, 2)), (80, 72124.42, datetime.date(2004, 11, 7)),
            (82, 28651.96, datetime.date(2007, 9, 10)), (83, 43798.7, datetime.date(2002, 4, 28)),
            (91, 13878.8, datetime.date(2001, 6, 4)), (94, 24895.13, datetime.date(2005, 12, 18)),
            (106, 28436.79, datetime.date(2009, 11, 8)), (110, 167901.18, datetime.date(2005, 8, 8)),
            (119, 57128.47, datetime.date(2006, 11, 30)), (128, 61008.57, datetime.date(2002, 9, 11)),
            (131, 138597.33, datetime.date(2005, 10, 23)), (134, 172066.33, datetime.date(2001, 4, 2)),
            (136, 140738.02, datetime.date(2002, 7, 7)), (140, 18555.63, datetime.date(2005, 3, 6)),
            (142, 11582.26, datetime.date(1992, 5, 16)), (149, 38797.9, datetime.date(2003, 11, 8)),
            (153, 172026.79, datetime.date(2000, 2, 2)), (154, 13151.54, datetime.date(2001, 12, 3)),
            (156, 167071.02, datetime.date(2000, 5, 20)), (158, 234348.24, datetime.date(2000, 4, 19)),
            (159, 50547.74, datetime.date(2000, 5, 7)), (161, 104060.07, datetime.date(2000, 5, 22)),
            (162, 181536.15, datetime.date(2000, 5, 26)), (164, 12849.09, datetime.date(2000, 7, 27)),
            (166, 114691.87, datetime.date(2000, 8, 14)), (168, 109061.19, datetime.date(2006, 6, 8)),
            (171, 60726.38, datetime.date(2000, 12, 5)), (172, 100311.59, datetime.date(2000, 12, 9)),
            (173, 48661.16, datetime.date(2000, 12, 23)), (176, 83160.75, datetime.date(2001, 2, 27)),
            (177, 150438.64, datetime.date(2008, 3, 11)), (178, 210767.49, datetime.date(2007, 6, 12)),
            (180, 66770.56, datetime.date(2001, 8, 25)), (182, 32608.4, datetime.date(2001, 10, 26)),
            (183, 77243.27, datetime.date(2001, 11, 10)), (187, 57790.55, datetime.date(2004, 6, 13)),
            (189, 38913.88, datetime.date(2002, 2, 5)), (193, 99254.84, datetime.date(2007, 4, 17)),
            (194, 178925.64, datetime.date(2002, 4, 16)), (195, 36406.44, datetime.date(2002, 7, 5)),
            (196, 80597.66, datetime.date(2002, 8, 7)), (197, 112665.4, datetime.date(2002, 8, 18)),
            (198, 84074.2, datetime.date(2002, 9, 9)), (199, 119419.4, datetime.date(2002, 9, 23)),
            (200, 70003.01, datetime.date(2002, 10, 1)), (202, 60758.81, datetime.date(2002, 10, 15)),
            (203, 131282.98, datetime.date(2002, 10, 23)), (204, 149596.1, datetime.date(2002, 11, 21)),
            (205, 29549.69, datetime.date(2003, 1, 11)), (209, 123438.2, datetime.date(2003, 3, 25)),
            (210, 96625.04, datetime.date(2003, 4, 9)), (211, 141172.77, datetime.date(2007, 12, 21)),
            (212, 41099.94, datetime.date(2003, 4, 27)), (213, 60132.1, datetime.date(2003, 5, 1)),
            (214, 166315.0, datetime.date(2003, 5, 8)), (215, 27613.11, datetime.date(2003, 6, 13)),
            (218, 13849.56, datetime.date(2003, 10, 1)), (219, 139258.44, datetime.date(2003, 10, 16)),
            (220, 36439.33, datetime.date(2003, 11, 7)), (221, 149451.12, datetime.date(2003, 12, 9)),
            (222, 63709.49, datetime.date(2003, 12, 17)), (223, 112761.16, datetime.date(2003, 12, 28)),
            (224, 189661.68, datetime.date(2004, 2, 6)), (225, 52249.76, datetime.date(2004, 12, 29)),
            (226, 84053.31, datetime.date(2004, 4, 12)), (227, 179677.32, datetime.date(2004, 4, 19)),
            (229, 107106.88, datetime.date(2004, 5, 14)), (230, 79268.19, datetime.date(2004, 5, 22)),
            (231, 183174.34, datetime.date(2008, 9, 15)), (232, 87920.77, datetime.date(2004, 7, 16)),
            (233, 140048.95, datetime.date(2007, 6, 3)), (235, 110150.17, datetime.date(2004, 10, 18)),
            (237, 98159.43, datetime.date(2004, 12, 20)), (238, 61288.05, datetime.date(2005, 1, 7)),
            (240, 140314.01, datetime.date(2005, 3, 17)), (243, 24281.57, datetime.date(2005, 4, 12)),
            (246, 32198.84, datetime.date(2005, 6, 13)), (248, 173649.68, datetime.date(2005, 10, 15)),
            (249, 117318.05, datetime.date(2005, 11, 15)), (250, 122576.05, datetime.date(2005, 11, 21)),
            (254, 177320.66, datetime.date(2006, 3, 10)), (255, 181381.43, datetime.date(2006, 3, 17)),
            (256, 33929.37, datetime.date(2006, 4, 5)), (257, 26900.2, datetime.date(2006, 4, 12)),
            (259, 21076.91, datetime.date(2006, 5, 22)), (261, 196808.82, datetime.date(2006, 6, 9)),
            (263, 30739.76, datetime.date(2006, 7, 23)), (265, 82678.91, datetime.date(2007, 9, 30)),
            (268, 40422.94, datetime.date(2006, 11, 28)), (269, 57531.04, datetime.date(2006, 12, 9)),
            (273, 178285.29, datetime.date(2007, 1, 29)), (275, 42888.53, datetime.date(2007, 3, 21)),
            (276, 115768.35, datetime.date(2007, 3, 29)), (278, 107020.23, datetime.date(2007, 5, 15)),
            (282, 150669.32, datetime.date(2007, 7, 31)), (287, 134001.66, datetime.date(2009, 10, 26)),
            (288, 79090.45, datetime.date(2007, 12, 2)), (297, 147415.96, datetime.date(2008, 4, 30)),
            (298, 60025.27, datetime.date(2008, 7, 4)), (299, 113933.0, datetime.date(2008, 7, 15)),
            (301, 78053.32, datetime.date(2008, 9, 1)), (305, 104788.84, datetime.date(2009, 2, 10)),
            (310, 141376.88, datetime.date(2009, 2, 23)), (311, 40893.32, datetime.date(2009, 3, 13)),
            (314, 71272.29, datetime.date(2009, 5, 29)), (323, 11929.86, datetime.date(2009, 11, 2)),
        )),

    BenchmarkCase(
        id='B5_1', title='App time current / system time current',
        sql="""SELECT ps2.*
FROM partsupp FOR BUSINESS_TIME AS OF '[APP_TIME]' as ps1,
partsupp FOR BUSINESS_TIME AS OF '[APP_TIME]' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey""",
        parameters={'PART_KEY': 1, 'APP_TIME': datetime.date(1997, 6, 1)},
        select=('ps_partkey',), distinct=True,
        rows=(
            (3,), (5,), (7,), (9,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,), (20,), (21,),
            (23,), (25,), (27,), (29,), (31,), (32,), (33,), (35,), (36,), (37,), (38,), (40,), (41,), (43,), (45,),
            (47,), (49,), (51,), (52,), (53,), (54,), (55,), (56,), (57,), (58,), (59,), (60,), (61,), (63,), (65,),
            (67,), (69,), (72,), (73,), (74,), (75,), (76,), (77,), (78,), (79,), (80,), (81,), (83,), (85,), (87,),
            (91,), (92,), (93,), (94,), (95,), (97,), (98,), (99,), (100,), (101,), (103,), (105,), (107,), (109,),
            (111,), (112,), (114,), (115,), (116,), (117,), (118,), (119,), (120,), (121,), (123,), (125,), (127,),
            (129,), (131,), (132,), (133,), (135,), (136,), (137,), (138,), (140,), (141,), (143,), (145,), (147,),
            (149,), (151,), (152,), (153,), (154,), (155,), (156,), (157,), (158,), (159,), (160,), (161,), (163,),
            (165,), (167,), (169,), (171,), (172,), (173,), (174,), (175,), (176,), (177,), (178,), (179,), (180,),
            (181,), (183,), (185,), (187,), (191,), (192,), (193,), (194,), (195,), (196,), (197,), (198,), (199,),
            (200,),
        )),

    BenchmarkCase(
        id='B5_2', title='App time past / system time past',
        sql="""SELECT ps2.*, ps1.ps_suppkey, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1.validity_time_end
FROM partsupp
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
FOR BUSINESS_TIME AS OF '[APP_TIME1]' as ps1,
partsupp
FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]'
FOR BUSINESS_TIME AS OF '[APP_TIME2]' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey""",
        parameters={'PART_KEY': 1, 'SYS_TIME': datetime.datetime(2005, 6, 1, 0, 0), 'APP_TIME1': datetime.date(1995, 6, 1), 'APP_TIME2': datetime.date(1997, 6, 1)},
        select=('ps_partkey',), distinct=True,
        rows=(
            (3,), (5,), (7,), (9,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,), (20,), (21,),
            (23,), (25,), (27,), (29,), (31,), (32,), (33,), (35,), (36,), (37,), (38,), (40,), (41,), (43,), (45,),
            (47,), (49,), (51,), (52,), (53,), (54,), (55,), (56,), (57,), (58,), (59,), (60,), (61,), (63,), (65,),
            (67,), (69,), (72,), (73,), (74,), (75,), (76,), (77,), (78,), (79,), (80,), (81,), (83,), (85,), (87,),
            (91,), (92,), (93,), (94,), (95,), (97,), (98,), (99,), (100,), (101,), (103,), (105,), (107,), (109,),
            (111,), (112,), (113,), (114,), (115,), (116,), (117,), (118,), (119,), (120,), (121,), (123,), (125,),
            (127,), (129,), (131,), (132,), (133,), (135,), (136,), (137,), (138,), (140,), (141,), (143,), (145,),
            (147,), (149,), (151,), (152,), (153,), (154,), (155,), (156,), (157,), (158,), (159,), (160,), (161,),
            (163,), (165,), (167,), (169,), (171,), (172,), (173,), (174,), (175,), (176,), (177,), (178,), (179,),
            (180,), (181,), (183,), (185,), (187,), (191,), (192,), (193,), (194,), (195,), (196,), (197,), (198,),
            (199,), (200,),
        )),

    BenchmarkCase(
        id='B5_3', title='App time correlation / system time current',
        sql="""SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1. validity_time_end
FROM partsupp FOR BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30' as ps1,
partsupp FOR BUSINESS_TIME FROM '0001-01-01' TO '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.VALIDITY_TIME_START <= ps2.VALIDITY_TIME_END AND
ps1.VALIDITY_TIME_END >= ps2.VALIDITY_TIME_START""",
        parameters={'PART_KEY': 1},
        select=('ps_partkey',), distinct=True,
        rows=(
            (3,), (5,), (7,), (9,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,), (20,), (21,),
            (23,), (25,), (27,), (29,), (31,), (32,), (33,), (35,), (36,), (37,), (38,), (40,), (41,), (43,), (45,),
            (47,), (49,), (51,), (52,), (53,), (54,), (55,), (56,), (57,), (58,), (59,), (60,), (61,), (63,), (65,),
            (67,), (69,), (71,), (72,), (73,), (74,), (75,), (76,), (77,), (78,), (79,), (80,), (81,), (83,), (85,),
            (87,), (91,), (92,), (93,), (94,), (95,), (96,), (97,), (98,), (99,), (100,), (101,), (103,), (105,),
            (107,), (109,), (111,), (112,), (113,), (114,), (115,), (116,), (117,), (118,), (119,), (120,), (121,),
            (123,), (125,), (127,), (129,), (131,), (132,), (133,), (135,), (136,), (137,), (138,), (140,), (141,),
            (143,), (145,), (147,), (149,), (151,), (152,), (153,), (154,), (155,), (156,), (157,), (158,), (159,),
            (160,), (161,), (163,), (165,), (167,), (169,), (171,), (172,), (173,), (174,), (175,), (176,), (177,),
            (178,), (179,), (180,), (181,), (183,), (185,), (187,), (191,), (192,), (193,), (194,), (195,), (196,),
            (197,), (198,), (199,), (200,),
        )),

    BenchmarkCase(
        id='B5_4', title='App time current / system time correlation',
        sql="""SELECT ps2.*, ps1.validity_time_start, ps1.validity_time_end
FROM partsupp FOR BUSINESS_TIME AS OF '[APP_TIME]'
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as PS1,
partsupp FOR BUSINESS_TIME AS OF '[APP_TIME]'
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as PS2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start < ps2.sys_time_END AND
ps2.sys_time_start < ps1.sys_time_END""",
        parameters={'PART_KEY': 1, 'APP_TIME': datetime.date(1997, 6, 1)},
        select=('ps_partkey',), distinct=True,
        rows=(
            (3,), (5,), (7,), (9,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,), (20,), (21,),
            (23,), (25,), (27,), (29,), (31,), (32,), (33,), (35,), (36,), (37,), (38,), (40,), (41,), (43,), (45,),
            (47,), (49,), (51,), (52,), (53,), (54,), (55,), (56,), (57,), (58,), (59,), (60,), (61,), (63,), (65,),
            (67,), (69,), (71,), (72,), (73,), (74,), (75,), (76,), (77,), (78,), (79,), (80,), (81,), (83,), (85,),
            (87,), (91,), (92,), (93,), (94,), (95,), (96,), (97,), (98,), (99,), (100,), (101,), (103,), (105,),
            (107,), (109,), (111,), (112,), (113,), (114,), (115,), (116,), (117,), (118,), (119,), (120,), (121,),
            (123,), (125,), (127,), (129,), (131,), (132,), (133,), (135,), (136,), (137,), (138,), (140,), (141,),
            (143,), (145,), (147,), (149,), (151,), (152,), (153,), (154,), (155,), (156,), (157,), (158,), (159,),
            (160,), (161,), (163,), (165,), (167,), (169,), (171,), (172,), (173,), (174,), (175,), (176,), (177,),
            (178,), (179,), (180,), (181,), (183,), (185,), (187,), (191,), (192,), (193,), (194,), (195,), (196,),
            (197,), (198,), (199,), (200,),
        )),

    BenchmarkCase(
        id='B5_5', title='App time correlation / system time correlation',
        sql="""SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1. validity_time_end
FROM partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps1,
partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start < ps2.sys_time_END AND
ps2.sys_time_start < ps1.sys_time_END AND
ps1.VALIDITY_TIME_START <= ps2.VALIDITY_TIME_END AND
ps1.VALIDITY_TIME_END >= ps2.VALIDITY_TIME_START""",
        parameters={'PART_KEY': 1},
        select=('ps_partkey',), distinct=True,
        rows=(
            (3,), (5,), (7,), (9,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,), (20,), (21,),
            (23,), (25,), (27,), (29,), (31,), (32,), (33,), (35,), (36,), (37,), (38,), (40,), (41,), (43,), (45,),
            (47,), (49,), (51,), (52,), (53,), (54,), (55,), (56,), (57,), (58,), (59,), (60,), (61,), (63,), (65,),
            (67,), (69,), (71,), (72,), (73,), (74,), (75,), (76,), (77,), (78,), (79,), (80,), (81,), (83,), (85,),
            (87,), (91,), (92,), (93,), (94,), (95,), (96,), (97,), (98,), (99,), (100,), (101,), (103,), (105,),
            (107,), (109,), (111,), (112,), (113,), (114,), (115,), (116,), (117,), (118,), (119,), (120,), (121,),
            (123,), (125,), (127,), (129,), (131,), (132,), (133,), (135,), (136,), (137,), (138,), (140,), (141,),
            (143,), (145,), (147,), (149,), (151,), (152,), (153,), (154,), (155,), (156,), (157,), (158,), (159,),
            (160,), (161,), (163,), (165,), (167,), (169,), (171,), (172,), (173,), (174,), (175,), (176,), (177,),
            (178,), (179,), (180,), (181,), (183,), (185,), (187,), (191,), (192,), (193,), (194,), (195,), (196,),
            (197,), (198,), (199,), (200,),
        )),

    BenchmarkCase(
        id='B5_6', title='App time correlation / system time correlation, as currently known',
        sql="""SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1. validity_time_end
FROM partsupp as ps1, partsupp as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey""",
        parameters={'PART_KEY': 1},
        select=('ps_partkey',), distinct=True,
        rows=(
            (3,), (5,), (7,), (9,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,), (20,), (21,),
            (23,), (25,), (27,), (29,), (31,), (32,), (33,), (35,), (36,), (37,), (38,), (40,), (41,), (43,), (45,),
            (47,), (49,), (51,), (52,), (53,), (54,), (55,), (56,), (57,), (58,), (59,), (60,), (61,), (63,), (65,),
            (67,), (69,), (71,), (72,), (73,), (74,), (75,), (76,), (77,), (78,), (79,), (80,), (81,), (83,), (85,),
            (87,), (91,), (92,), (93,), (94,), (95,), (96,), (97,), (98,), (99,), (100,), (101,), (103,), (105,),
            (107,), (109,), (111,), (112,), (113,), (114,), (115,), (116,), (117,), (118,), (119,), (120,), (121,),
            (123,), (125,), (127,), (129,), (131,), (132,), (133,), (135,), (136,), (137,), (138,), (140,), (141,),
            (143,), (145,), (147,), (149,), (151,), (152,), (153,), (154,), (155,), (156,), (157,), (158,), (159,),
            (160,), (161,), (163,), (165,), (167,), (169,), (171,), (172,), (173,), (174,), (175,), (176,), (177,),
            (178,), (179,), (180,), (181,), (183,), (185,), (187,), (191,), (192,), (193,), (194,), (195,), (196,),
            (197,), (198,), (199,), (200,),
        )),

    BenchmarkCase(
        id='B5_7', title='App time agnostic / system time past',
        sql="""SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1.validity_time_end
FROM partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' as ps1,
partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '[SYS_TIME]' as ps2
WHERE ps1.ps_partkey = [PART_KEY]
AND ps1.ps_suppkey = ps2.ps_suppkey
AND ps1.ps_partkey <> ps2.ps_partkey""",
        parameters={'PART_KEY': 1, 'SYS_TIME': datetime.datetime(2002, 6, 1, 0, 0)},
        select=('ps_partkey',), distinct=True,
        rows=(
            (3,), (5,), (7,), (9,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,), (20,), (21,),
            (23,), (25,), (27,), (29,), (31,), (32,), (33,), (35,), (36,), (37,), (38,), (40,), (41,), (43,), (45,),
            (47,), (49,), (51,), (52,), (53,), (54,), (55,), (56,), (57,), (58,), (59,), (60,), (61,), (63,), (65,),
            (67,), (69,), (71,), (72,), (73,), (74,), (75,), (76,), (77,), (78,), (79,), (80,), (81,), (83,), (85,),
            (87,), (91,), (92,), (93,), (94,), (95,), (96,), (97,), (98,), (99,), (100,), (101,), (103,), (105,),
            (107,), (109,), (111,), (112,), (113,), (114,), (115,), (116,), (117,), (118,), (119,), (120,), (121,),
            (123,), (125,), (127,), (129,), (131,), (132,), (133,), (135,), (136,), (137,), (138,), (140,), (141,),
            (143,), (145,), (147,), (149,), (151,), (152,), (153,), (154,), (155,), (156,), (157,), (158,), (159,),
            (160,), (161,), (163,), (165,), (167,), (169,), (171,), (172,), (173,), (174,), (175,), (176,), (177,),
            (178,), (179,), (180,), (181,), (183,), (185,), (187,), (191,), (192,), (193,), (194,), (195,), (196,),
            (197,), (198,), (199,), (200,),
        )),

    BenchmarkCase(
        id='B5_8', title='App time agnostic / system time correlation',
        sql="""SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1. validity_time_start, ps1. validity_time_end
FROM partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps1,
partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start < ps2.sys_time_END AND
ps2.sys_time_start < ps1.sys_time_END""",
        parameters={'PART_KEY': 1},
        select=('ps_partkey',), distinct=True,
        rows=(
            (3,), (5,), (7,), (9,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,), (20,), (21,),
            (23,), (25,), (27,), (29,), (31,), (32,), (33,), (35,), (36,), (37,), (38,), (40,), (41,), (43,), (45,),
            (47,), (49,), (51,), (52,), (53,), (54,), (55,), (56,), (57,), (58,), (59,), (60,), (61,), (63,), (65,),
            (67,), (69,), (71,), (72,), (73,), (74,), (75,), (76,), (77,), (78,), (79,), (80,), (81,), (83,), (85,),
            (87,), (91,), (92,), (93,), (94,), (95,), (96,), (97,), (98,), (99,), (100,), (101,), (103,), (105,),
            (107,), (109,), (111,), (112,), (113,), (114,), (115,), (116,), (117,), (118,), (119,), (120,), (121,),
            (123,), (125,), (127,), (129,), (131,), (132,), (133,), (135,), (136,), (137,), (138,), (140,), (141,),
            (143,), (145,), (147,), (149,), (151,), (152,), (153,), (154,), (155,), (156,), (157,), (158,), (159,),
            (160,), (161,), (163,), (165,), (167,), (169,), (171,), (172,), (173,), (174,), (175,), (176,), (177,),
            (178,), (179,), (180,), (181,), (183,), (185,), (187,), (191,), (192,), (193,), (194,), (195,), (196,),
            (197,), (198,), (199,), (200,),
        )),

    BenchmarkCase(
        id='B5_9', title='App time current / system time agnostic',
        sql="""SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1.validity_time_end
FROM partsupp
FOR BUSINESS_TIME AS OF '[APP_TIME]'
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps1,
partsupp
FOR BUSINESS_TIME AS OF '[APP_TIME]'
FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start <= ps2.sys_time_START AND
ps2.sys_time_start < ps1.sys_time_END""",
        parameters={'PART_KEY': 1, 'APP_TIME': datetime.date(1997, 6, 1)},
        select=('ps_partkey',), distinct=True,
        rows=(
            (3,), (5,), (7,), (9,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,), (20,), (21,),
            (23,), (25,), (27,), (29,), (31,), (32,), (33,), (35,), (36,), (37,), (38,), (40,), (41,), (43,), (45,),
            (47,), (49,), (51,), (52,), (53,), (54,), (55,), (56,), (57,), (58,), (59,), (60,), (61,), (63,), (65,),
            (67,), (69,), (71,), (72,), (73,), (74,), (75,), (76,), (77,), (78,), (79,), (80,), (81,), (83,), (85,),
            (87,), (91,), (92,), (93,), (94,), (95,), (96,), (97,), (98,), (99,), (100,), (101,), (103,), (105,),
            (107,), (109,), (111,), (112,), (113,), (114,), (115,), (116,), (117,), (118,), (119,), (120,), (121,),
            (123,), (125,), (127,), (129,), (131,), (132,), (133,), (135,), (136,), (137,), (138,), (140,), (141,),
            (143,), (145,), (147,), (149,), (151,), (152,), (153,), (154,), (155,), (156,), (157,), (158,), (159,),
            (160,), (161,), (163,), (165,), (167,), (169,), (171,), (172,), (173,), (174,), (175,), (176,), (177,),
            (178,), (179,), (180,), (181,), (183,), (185,), (187,), (191,), (192,), (193,), (194,), (195,), (196,),
            (197,), (198,), (199,), (200,),
        )),

    BenchmarkCase(
        id='B5_10', title='App time correlation / system time agnostic',
        sql="""SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1. validity_time_end
FROM partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps1,
partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start <= ps2.sys_time_START AND
pS2.sys_time_start < ps1.sys_time_END AND
ps1.VALIDITY_TIME_START <= ps2.VALIDITY_TIME_END AND
ps1.VALIDITY_TIME_END >= ps2.VALIDITY_TIME_START""",
        parameters={'PART_KEY': 1},
        select=('ps_partkey',), distinct=True,
        rows=(
            (3,), (5,), (7,), (9,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,), (20,), (21,),
            (23,), (25,), (27,), (29,), (31,), (32,), (33,), (35,), (36,), (37,), (38,), (40,), (41,), (43,), (45,),
            (47,), (49,), (51,), (52,), (53,), (54,), (55,), (56,), (57,), (58,), (59,), (60,), (61,), (63,), (65,),
            (67,), (69,), (71,), (72,), (73,), (74,), (75,), (76,), (77,), (78,), (79,), (80,), (81,), (83,), (85,),
            (87,), (91,), (92,), (93,), (94,), (95,), (96,), (97,), (98,), (99,), (100,), (101,), (103,), (105,),
            (107,), (109,), (111,), (112,), (113,), (114,), (115,), (116,), (117,), (118,), (119,), (120,), (121,),
            (123,), (125,), (127,), (129,), (131,), (132,), (133,), (135,), (136,), (137,), (138,), (140,), (141,),
            (143,), (145,), (147,), (149,), (151,), (152,), (153,), (154,), (155,), (156,), (157,), (158,), (159,),
            (160,), (161,), (163,), (165,), (167,), (169,), (171,), (172,), (173,), (174,), (175,), (176,), (177,),
            (178,), (179,), (180,), (181,), (183,), (185,), (187,), (191,), (192,), (193,), (194,), (195,), (196,),
            (197,), (198,), (199,), (200,),
        )),

    BenchmarkCase(
        id='B5_11', title='App time agnostic / system time agnostic',
        sql="""SELECT ps2.*, ps1.sys_time_start, ps1.sys_time_end,
ps1.validity_time_start, ps1. validity_time_end
FROM partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps1,
partsupp FOR SYSTEM_TIME BETWEEN '0001-01-01' AND '9999-12-30' as ps2
WHERE ps1.ps_partkey = [PART_KEY] AND
ps1.ps_suppkey = ps2.ps_suppkey AND
ps1.ps_partkey <> ps2.ps_partkey AND
ps1.sys_time_start <= ps2.sys_time_START AND
ps2.sys_time_start < ps1.sys_time_END""",
        parameters={'PART_KEY': 1},
        select=('ps_partkey',), distinct=True,
        rows=(
            (3,), (5,), (7,), (9,), (11,), (12,), (13,), (14,), (15,), (16,), (17,), (18,), (19,), (20,), (21,),
            (23,), (25,), (27,), (29,), (31,), (32,), (33,), (35,), (36,), (37,), (38,), (40,), (41,), (43,), (45,),
            (47,), (49,), (51,), (52,), (53,), (54,), (55,), (56,), (57,), (58,), (59,), (60,), (61,), (63,), (65,),
            (67,), (69,), (71,), (72,), (73,), (74,), (75,), (76,), (77,), (78,), (79,), (80,), (81,), (83,), (85,),
            (87,), (91,), (92,), (93,), (94,), (95,), (96,), (97,), (98,), (99,), (100,), (101,), (103,), (105,),
            (107,), (109,), (111,), (112,), (113,), (114,), (115,), (116,), (117,), (118,), (119,), (120,), (121,),
            (123,), (125,), (127,), (129,), (131,), (132,), (133,), (135,), (136,), (137,), (138,), (140,), (141,),
            (143,), (145,), (147,), (149,), (151,), (152,), (153,), (154,), (155,), (156,), (157,), (158,), (159,),
            (160,), (161,), (163,), (165,), (167,), (169,), (171,), (172,), (173,), (174,), (175,), (176,), (177,),
            (178,), (179,), (180,), (181,), (183,), (185,), (187,), (191,), (192,), (193,), (194,), (195,), (196,),
            (197,), (198,), (199,), (200,),
        )),
]}
