CREATE TABLE `customer_summary` (
  `Customer` text DEFAULT NULL,
  `Quantity` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `sales_orders` (
  `ID` int(11) DEFAULT NULL,
  `Customer` text DEFAULT NULL,
  `Product` text DEFAULT NULL,
  `Date` text DEFAULT NULL,
  `Quantity` int(11) DEFAULT NULL,
  `Rate` double DEFAULT NULL,
  `Tags` text DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;