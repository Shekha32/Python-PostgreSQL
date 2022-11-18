create table car (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	make VARCHAR(20) NOT NULL,
	model VARCHAR(50) NOT NULL,
	year INT NOT NULL,
	vin VARCHAR(50) NOT NULL,
	color VARCHAR(20) NOT NULL,
	price NUMERIC(6, 2) NOT NULL
);
insert into car (id, make, model, year, vin, color, price) values (1, 'Pontiac', 'Grand Prix', 2002, '4T1BF3EK7BU813801', 'Violet', '4343.75');
insert into car (id, make, model, year, vin, color, price) values (2, 'Mazda', 'MPV', 2003, '1FMJK1F57AE443041', 'Crimson', '3633.18');
insert into car (id, make, model, year, vin, color, price) values (3, 'Dodge', 'Daytona', 1984, '55SWF4JB7FU675014', 'Mauv', '4338.90');
insert into car (id, make, model, year, vin, color, price) values (4, 'Toyota', 'Celica', 1999, '2G4GR5EC1B9005460', 'Blue', '2237.39');
insert into car (id, make, model, year, vin, color, price) values (5, 'Mazda', 'B-Series', 1999, '5N1AT2ML2EC624816', 'Crimson', '4700.78');
insert into car (id, make, model, year, vin, color, price) values (6, 'Chevrolet', 'Express 1500', 2012, 'KNAFU5A24D5462045', 'Red', '4019.21');
insert into car (id, make, model, year, vin, color, price) values (7, 'Dodge', 'Caravan', 1997, 'WP0AA2A9XAS822885', 'Violet', '3299.50');
insert into car (id, make, model, year, vin, color, price) values (8, 'Audi', 'A8', 1999, '2C3CCAPG3DH911703', 'Orange', '4015.42');
insert into car (id, make, model, year, vin, color, price) values (9, 'Saab', '9000', 1993, '3C3CFFFHXDT394843', 'Blue', '4673.71');
insert into car (id, make, model, year, vin, color, price) values (10, 'Saab', '9-5', 2007, '3G5DB03L06S472774', 'Maroon', '2097.68');
insert into car (id, make, model, year, vin, color, price) values (11, 'Mazda', '626', 2002, 'TRUWT28N821204500', 'Crimson', '2628.93');
insert into car (id, make, model, year, vin, color, price) values (12, 'BMW', '600', 1957, 'WAUKF78E25A964713', 'Puce', '2774.95');
insert into car (id, make, model, year, vin, color, price) values (13, 'Lotus', 'Esprit', 1987, 'WBA3C1C52DF709949', 'Pink', '4377.79');
insert into car (id, make, model, year, vin, color, price) values (14, 'Jeep', 'Wrangler', 2000, 'JTHBB1BA6B2810477', 'Yellow', '2304.60');
insert into car (id, make, model, year, vin, color, price) values (15, 'Nissan', 'Xterra', 2011, 'SALWR2TF3FA938405', 'Goldenrod', '2423.32');
insert into car (id, make, model, year, vin, color, price) values (16, 'Suzuki', 'SX4', 2010, 'KMHCT4AE3CU120227', 'Maroon', '4663.96');
insert into car (id, make, model, year, vin, color, price) values (17, 'Ford', 'Ranger', 1996, '5J8TB4H51DL662965', 'Mauv', '4224.59');
insert into car (id, make, model, year, vin, color, price) values (18, 'Chevrolet', 'Caprice', 1991, 'WAUPEBFM2DA362897', 'Puce', '2786.71');
insert into car (id, make, model, year, vin, color, price) values (19, 'Saab', '9000', 1987, 'WBANW1C53AC972786', 'Indigo', '4351.45');
insert into car (id, make, model, year, vin, color, price) values (20, 'Mercury', 'Cougar', 1986, 'WAUFEAFM6AA720794', 'Violet', '3073.52');
insert into car (id, make, model, year, vin, color, price) values (21, 'Ford', 'Contour', 1995, '3D7TT2HT9BG442875', 'Teal', '2983.17');
insert into car (id, make, model, year, vin, color, price) values (22, 'Maserati', 'Karif', 1989, 'WBAWV1C5XAP621549', 'Orange', '4378.08');
insert into car (id, make, model, year, vin, color, price) values (23, 'Honda', 'Passport', 1996, '1D7RB1CTXAS550086', 'Blue', '3118.27');
insert into car (id, make, model, year, vin, color, price) values (24, 'Ford', 'Expedition', 2009, 'WAUHFAFL8BN554026', 'Puce', '4709.33');
insert into car (id, make, model, year, vin, color, price) values (25, 'Chevrolet', 'Astro', 1994, 'WBASP4C57CC997968', 'Crimson', '2004.87');
insert into car (id, make, model, year, vin, color, price) values (26, 'Chrysler', 'Sebring', 1998, 'WAUKH94F37N376075', 'Green', '3625.36');
insert into car (id, make, model, year, vin, color, price) values (27, 'Rolls-Royce', 'Ghost', 2010, '1GYS3CEF1BR935857', 'Fuscia', '2017.70');
insert into car (id, make, model, year, vin, color, price) values (28, 'Buick', 'Electra', 1986, '2HNYD28857H739183', 'Goldenrod', '3954.77');
insert into car (id, make, model, year, vin, color, price) values (29, 'Ford', 'Taurus', 2002, '1G6DK8E37D0779208', 'Mauv', '4922.56');
insert into car (id, make, model, year, vin, color, price) values (30, 'Cadillac', 'Escalade EXT', 2004, 'WA1BV74L17D821600', 'Indigo', '2092.38');
insert into car (id, make, model, year, vin, color, price) values (31, 'Mercedes-Benz', 'R-Class', 2007, '1HGCR2F35FA002685', 'Crimson', '4398.97');
insert into car (id, make, model, year, vin, color, price) values (32, 'Volkswagen', 'Corrado', 1994, '1FTSW2A51AE025486', 'Pink', '3786.50');
insert into car (id, make, model, year, vin, color, price) values (33, 'Maybach', '62', 2005, '5UXZW0C56BL311252', 'Goldenrod', '4336.95');
insert into car (id, make, model, year, vin, color, price) values (34, 'Volkswagen', 'New Beetle', 2002, 'SAJWA4EB4EL224534', 'Aquamarine', '4624.07');
insert into car (id, make, model, year, vin, color, price) values (35, 'BMW', '3 Series', 2011, '1C4AJWAG0EL800190', 'Pink', '4165.15');
insert into car (id, make, model, year, vin, color, price) values (36, 'Nissan', '350Z', 2005, '1G6AB5SX3D0953154', 'Yellow', '4894.66');
insert into car (id, make, model, year, vin, color, price) values (37, 'Chevrolet', 'Express 1500', 1999, 'WAUKH78E56A304810', 'Blue', '2761.89');
insert into car (id, make, model, year, vin, color, price) values (38, 'BMW', '5 Series', 1999, 'WBA3B1C53DK712210', 'Khaki', '4436.83');
insert into car (id, make, model, year, vin, color, price) values (39, 'Pontiac', 'Grand Prix', 1985, '1GTN1UEH2FZ669688', 'Violet', '2996.11');
insert into car (id, make, model, year, vin, color, price) values (40, 'Mercedes-Benz', 'W201', 1989, 'JTMHY7AJ5B5166127', 'Mauv', '4800.23');
insert into car (id, make, model, year, vin, color, price) values (41, 'Mitsubishi', 'Outlander Sport', 2011, '3D7TP2CT6BG661837', 'Red', '2342.13');
insert into car (id, make, model, year, vin, color, price) values (42, 'Chevrolet', 'Corvette', 2008, 'WBA4B1C58FG469584', 'Fuscia', '3688.82');
insert into car (id, make, model, year, vin, color, price) values (43, 'Toyota', 'Celica', 2000, 'WBAYE8C51ED920923', 'Pink', '4314.17');
insert into car (id, make, model, year, vin, color, price) values (44, 'Kia', 'Sorento', 2005, 'WAUEFAFL6FA163070', 'Maroon', '4175.77');
insert into car (id, make, model, year, vin, color, price) values (45, 'Suzuki', 'Vitara', 1999, '1N6AA0EC9EN838863', 'Puce', '2278.28');
insert into car (id, make, model, year, vin, color, price) values (46, 'Nissan', '240SX', 1998, 'WA1MYAFE9AD144640', 'Indigo', '3426.36');
insert into car (id, make, model, year, vin, color, price) values (47, 'Ford', 'Tempo', 1984, '4T1BK3DB6AU343658', 'Goldenrod', '4146.29');
insert into car (id, make, model, year, vin, color, price) values (48, 'Mazda', 'Millenia', 2001, 'WAURV78T89A895953', 'Maroon', '4901.66');
insert into car (id, make, model, year, vin, color, price) values (49, 'Nissan', 'Altima', 1995, '1GTN1TEHXFZ055997', 'Puce', '2458.24');
insert into car (id, make, model, year, vin, color, price) values (50, 'Lexus', 'SC', 1999, '5UXWX5C50CL312508', 'Fuscia', '4495.92');
insert into car (id, make, model, year, vin, color, price) values (51, 'Volkswagen', 'Jetta', 2009, '19VDE1F72DE973027', 'Indigo', '2691.98');
insert into car (id, make, model, year, vin, color, price) values (52, 'Audi', 'A8', 1998, 'WAUGVAFR9CA822612', 'Orange', '3310.73');
insert into car (id, make, model, year, vin, color, price) values (53, 'BMW', 'X6', 2008, '1GD010CG4BF717194', 'Blue', '4134.16');
insert into car (id, make, model, year, vin, color, price) values (54, 'Nissan', 'Xterra', 2001, '2T1KE4EE7DC036740', 'Orange', '4052.69');
insert into car (id, make, model, year, vin, color, price) values (55, 'Honda', 'Civic Si', 2005, '5FRYD4H81EB404084', 'Maroon', '3218.97');
insert into car (id, make, model, year, vin, color, price) values (56, 'Pontiac', 'Grand Prix', 1970, '1GTN2TEH8FZ586161', 'Red', '3618.32');
insert into car (id, make, model, year, vin, color, price) values (57, 'Cadillac', 'Escalade', 2010, 'JH4DC54842C184686', 'Pink', '4437.56');
insert into car (id, make, model, year, vin, color, price) values (58, 'Pontiac', 'Grand Prix', 1984, 'SCBCU7ZA6BC585368', 'Maroon', '2882.08');
insert into car (id, make, model, year, vin, color, price) values (59, 'GMC', 'Yukon', 2001, '5GAKRCKD0DJ367806', 'Mauv', '2081.21');
insert into car (id, make, model, year, vin, color, price) values (60, 'BMW', '645', 2004, 'JHMZF1C65FS037701', 'Violet', '3015.42');
insert into car (id, make, model, year, vin, color, price) values (61, 'Dodge', 'Grand Caravan', 1992, 'WBAEV53465K714159', 'Fuscia', '3829.98');
insert into car (id, make, model, year, vin, color, price) values (62, 'Dodge', 'Ram Van B150', 1994, 'JTJBJRBZ3F2275457', 'Khaki', '4315.60');
insert into car (id, make, model, year, vin, color, price) values (63, 'Volvo', 'XC70', 2009, 'WAUUL68E05A277810', 'Red', '3074.40');
insert into car (id, make, model, year, vin, color, price) values (64, 'Mercedes-Benz', 'E-Class', 2011, 'WAUDGBFL0BA289241', 'Maroon', '4334.85');
insert into car (id, make, model, year, vin, color, price) values (65, 'Chevrolet', 'Camaro', 1978, 'WBALW3C5XEC074579', 'Maroon', '3154.83');
insert into car (id, make, model, year, vin, color, price) values (66, 'Buick', 'Park Avenue', 1993, '1G4CW54K614981333', 'Purple', '4700.71');
insert into car (id, make, model, year, vin, color, price) values (67, 'Chevrolet', 'Impala', 2010, 'JH4DC54852C026616', 'Maroon', '2015.55');
insert into car (id, make, model, year, vin, color, price) values (68, 'Suzuki', 'Cultus', 1985, '2HNYD2H23CH466864', 'Mauv', '2392.51');
insert into car (id, make, model, year, vin, color, price) values (69, 'Dodge', 'Ram Van 3500', 1997, '3D4PG3FG4BT547121', 'Purple', '4588.09');
insert into car (id, make, model, year, vin, color, price) values (70, 'Ford', 'E-Series', 2010, 'SCBDU3ZA2CC942110', 'Indigo', '3497.46');
insert into car (id, make, model, year, vin, color, price) values (71, 'Dodge', 'Caliber', 2011, '3VW8S7AT0FM137246', 'Yellow', '3726.41');
insert into car (id, make, model, year, vin, color, price) values (72, 'Hummer', 'H3', 2009, '3D7TP2CT3BG840532', 'Crimson', '4263.71');
insert into car (id, make, model, year, vin, color, price) values (73, 'Toyota', 'TundraMax', 2007, '2T1BURHE1FC485023', 'Green', '4915.51');
insert into car (id, make, model, year, vin, color, price) values (74, 'Oldsmobile', '98', 1994, '1FTWX3B55AE697067', 'Puce', '2877.90');
insert into car (id, make, model, year, vin, color, price) values (75, 'Subaru', 'Tribeca', 2011, 'WAUGFAFR7DA040288', 'Fuscia', '4202.88');
insert into car (id, make, model, year, vin, color, price) values (76, 'Cadillac', 'DeVille', 1993, 'WAUAGAFDXDN246432', 'Khaki', '4080.70');
insert into car (id, make, model, year, vin, color, price) values (77, 'Saturn', 'S-Series', 1992, 'WUATNAFG0EN066344', 'Mauv', '2321.94');
insert into car (id, make, model, year, vin, color, price) values (78, 'Spyker', 'C8', 2007, '1C6RD6KP1CS931986', 'Puce', '4648.71');
insert into car (id, make, model, year, vin, color, price) values (79, 'Chevrolet', 'Silverado 1500', 2012, 'WAULC58E65A288624', 'Yellow', '3370.39');
insert into car (id, make, model, year, vin, color, price) values (80, 'Volvo', 'V70', 2002, 'WAUTFAFH4DN492858', 'Turquoise', '3052.60');
insert into car (id, make, model, year, vin, color, price) values (81, 'GMC', 'Safari', 2002, 'WBAYE8C56FD991245', 'Pink', '2556.81');
insert into car (id, make, model, year, vin, color, price) values (82, 'Aston Martin', 'V8 Vantage', 2007, '1N6AD0CU0AC128923', 'Khaki', '3935.78');
insert into car (id, make, model, year, vin, color, price) values (83, 'Lexus', 'RX', 2013, '2T1BURHE0EC133632', 'Blue', '3228.77');
insert into car (id, make, model, year, vin, color, price) values (84, 'Chevrolet', 'Cavalier', 2002, '1C4RDJEG9EC997206', 'Red', '4286.44');
insert into car (id, make, model, year, vin, color, price) values (85, 'GMC', 'Sierra 1500', 2008, '3C6TD5NT0CG476743', 'Violet', '2454.68');
insert into car (id, make, model, year, vin, color, price) values (86, 'Chevrolet', 'Impala', 2011, '1FT7W2A61EE946224', 'Teal', '4264.05');
insert into car (id, make, model, year, vin, color, price) values (87, 'Plymouth', 'Grand Voyager', 2000, 'WBSBL93486P740794', 'Violet', '3527.90');
insert into car (id, make, model, year, vin, color, price) values (88, 'Mercury', 'Grand Marquis', 2003, '3GYVKMEF3AG800884', 'Goldenrod', '2597.93');
insert into car (id, make, model, year, vin, color, price) values (89, 'Isuzu', 'i-Series', 2008, '1G6DM8ED6B0287429', 'Blue', '3847.88');
insert into car (id, make, model, year, vin, color, price) values (90, 'Subaru', 'Impreza', 2002, '5N1AN0NU5FN357358', 'Red', '2030.23');
insert into car (id, make, model, year, vin, color, price) values (91, 'Volkswagen', 'R32', 2008, '2G4GR5ECXB9634621', 'Puce', '3958.13');
insert into car (id, make, model, year, vin, color, price) values (92, 'Ford', 'Explorer', 1994, 'WA1MMBFE5AD022584', 'Green', '3788.07');
insert into car (id, make, model, year, vin, color, price) values (93, 'Porsche', 'Boxster', 2005, '1FTMF1E81AF375976', 'Maroon', '3604.45');
insert into car (id, make, model, year, vin, color, price) values (94, 'Jaguar', 'XK', 2011, '1G4GC5GC5BF685983', 'Maroon', '2225.95');
insert into car (id, make, model, year, vin, color, price) values (95, 'Chevrolet', 'Silverado 3500', 2007, 'SCFAB01A56G292123', 'Pink', '3486.94');
insert into car (id, make, model, year, vin, color, price) values (96, 'Buick', 'Riviera', 1979, 'WVWAA7AJ0CW057656', 'Blue', '4130.41');
insert into car (id, make, model, year, vin, color, price) values (97, 'Chevrolet', 'Metro', 2000, '5XYKT3A19BG496065', 'Khaki', '3813.77');
insert into car (id, make, model, year, vin, color, price) values (98, 'Volkswagen', 'Passat', 1992, '3D73Y3HL3BG194419', 'Turquoise', '3880.26');
insert into car (id, make, model, year, vin, color, price) values (99, 'Infiniti', 'G', 1995, '5TDDCRFH8ES105934', 'Aquamarine', '3576.63');
insert into car (id, make, model, year, vin, color, price) values (100, 'Isuzu', 'Hombre', 1998, 'WAUVC68E85A403934', 'Pink', '2289.65');
